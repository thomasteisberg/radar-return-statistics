import logging

import numpy as np
import pandas as pd
import scipy.constants
import xarray as xr
from xopr import OPRConnection
from xopr import qc as xopr_qc

logger = logging.getLogger(__name__)

SURFACE_KEY = "standard:surface"
BED_KEY = "standard:bottom"


def extract_layer_peak_power(radar_ds, layer_twtt, margin_twtt):
    """Extract peak power (dB) and its TWTT within a margin around a layer pick."""
    t_start = np.minimum(radar_ds.slow_time.min(), layer_twtt.slow_time.min())
    t_end = np.maximum(radar_ds.slow_time.max(), layer_twtt.slow_time.max())
    layer_twtt = layer_twtt.sel(slow_time=slice(t_start, t_end))
    radar_ds = radar_ds.sel(slow_time=slice(t_start, t_end))
    layer_twtt = layer_twtt.reindex(
        slow_time=radar_ds.slow_time,
        method="nearest",
        tolerance=pd.Timedelta(seconds=1),
        fill_value=np.nan,
    )

    start_twtt = layer_twtt - margin_twtt
    end_twtt = layer_twtt + margin_twtt
    data_within_margin = radar_ds.where(
        (radar_ds.twtt >= start_twtt) & (radar_ds.twtt <= end_twtt),
        drop=True,
    )

    power_dB = 10 * np.log10(np.abs(data_within_margin.Data))
    peak_twtt_index = power_dB.argmax(dim="twtt")
    peak_twtt = power_dB.twtt[peak_twtt_index]
    peak_power = power_dB.isel(twtt=peak_twtt_index)

    peak_twtt = peak_twtt.drop_vars("twtt")
    peak_power = peak_power.drop_vars("twtt")

    return peak_twtt, peak_power


def _build_qc_checks(qc_config: dict) -> dict:
    """Build xopr QC checks dict from config. Only includes enabled (non-null) checks."""
    checks = {}

    val = qc_config.get("max_heading_change_deg_per_km")
    if val is not None:
        checks["heading_change"] = {"max_deg_per_km": val}

    val = qc_config.get("min_ice_thickness_m")
    if val is not None:
        checks["ice_thickness_threshold"] = {"min_thickness_m": val}

    val = qc_config.get("min_agl_m")
    if val is not None:
        checks["minimum_agl"] = {"min_agl_m": val}

    val = qc_config.get("min_bed_snr_db")
    if val is not None:
        checks["snr_bed_pick"] = {"min_snr_db": val}

    return checks


def process_frame(opr: OPRConnection, stac_item, config: dict) -> xr.Dataset | None:
    """Process a single radar frame and return a Dataset of metrics, or None on failure."""
    proc = config["processing"]
    qc_config = config.get("qc", {})
    frame_id = stac_item.name if hasattr(stac_item, "name") else stac_item.get("id", "unknown")

    try:
        frame = opr.load_frame(stac_item, data_product=proc["data_product"])
        frame = frame.sortby("slow_time")

        decimate_interval = proc.get("decimate_interval")
        if decimate_interval:
            interval = pd.Timedelta(decimate_interval)
            times = frame.slow_time.values
            selected = [0]
            last = times[0]
            for idx in range(1, len(times)):
                if times[idx] - last >= interval:
                    selected.append(idx)
                    last = times[idx]
            frame = frame.isel(slow_time=selected)

        try:
            layers = opr.get_layers(frame, include_geometry=False)
        except Exception:
            logger.warning("Frame %s: failed to load layers, skipping", frame_id)
            return None

        if layers is None or SURFACE_KEY not in layers or BED_KEY not in layers:
            available = list(layers.keys()) if layers else []
            logger.warning("Frame %s: missing layer picks (available: %s), skipping",
                           frame_id, available)
            return None

        # Add layer picks to frame so xopr QC checks can use them
        for key in (SURFACE_KEY, BED_KEY):
            pick = layers[key]["twtt"].reindex(
                slow_time=frame.slow_time,
                method="nearest",
                tolerance=pd.Timedelta(seconds=5),
                fill_value=np.nan,
            )
            frame[key] = pick

        # Run xopr QC checks (picks already in frame, ensure_picks is a no-op)
        qc_checks = _build_qc_checks(qc_config)
        if qc_checks:
            frame = xopr_qc.run_qc(frame, checks=qc_checks)
            qc_mask = frame["qc"]

            n_pass = int(qc_mask.sum())
            n_total = len(qc_mask)
            min_traces = qc_config.get("min_traces_after_qc", 10)
            if n_pass < min_traces:
                logger.warning("Frame %s: only %d/%d traces pass QC (need %d), skipping",
                               frame_id, n_pass, n_total, min_traces)
                return None
            if n_pass < n_total:
                logger.info("Frame %s: QC filtered %d/%d traces", frame_id, n_total - n_pass, n_total)
        else:
            qc_mask = None

        ice_permittivity = proc["ice_permittivity"]
        c = scipy.constants.c
        v_ice = c / np.sqrt(ice_permittivity)
        margin_twtt = proc["layer_margin_m"] / v_ice

        surface_twtt, surface_power = extract_layer_peak_power(
            frame, layers[SURFACE_KEY]["twtt"], margin_twtt
        )
        bed_twtt, bed_power = extract_layer_peak_power(
            frame, layers[BED_KEY]["twtt"], margin_twtt
        )

        surface_elevation = frame.Elevation - (c / 2) * surface_twtt
        bed_elevation = surface_elevation - (v_ice / 2) * (bed_twtt - surface_twtt)

        # Required surface SNR: surface-to-bed power ratio corrected for geometric spreading.
        # Matches the RSSNR definition from https://github.com/thomasteisberg/required_surface_snr
        r_surf = c * surface_twtt / 2  # one-way air range to surface (m)
        ice_thickness = v_ice / 2 * (bed_twtt - surface_twtt)  # one-way ice thickness (m)
        r_bed_eff = r_surf + ice_thickness / np.sqrt(ice_permittivity)
        P_surf_lin = 10 ** (surface_power / 10)
        P_bed_lin = 10 ** (bed_power / 10)
        with np.errstate(divide="ignore", invalid="ignore"):
            required_surface_snr_dB = 10 * np.log10(
                P_surf_lin * r_surf**2 / (P_bed_lin * r_bed_eff**2)
            )

        if qc_mask is not None:
            qc_pass = qc_mask
        else:
            qc_pass = xr.ones_like(frame.slow_time, dtype=bool)

        metric_vars = {
            "surface_twtt": surface_twtt,
            "bed_twtt": bed_twtt,
            "surface_elevation": surface_elevation,
            "bed_elevation": bed_elevation,
            "surface_power_dB": surface_power,
            "bed_power_dB": bed_power,
            "required_surface_snr_dB": required_surface_snr_dB,
        }

        if qc_mask is not None:
            for name in metric_vars:
                metric_vars[name] = metric_vars[name].where(qc_pass)

        ds = xr.Dataset(
            {
                **metric_vars,
                "qc_pass": qc_pass,
                "frame_id": ("slow_time", [str(frame_id)] * len(frame.slow_time)),
            },
            coords={
                "latitude": frame.Latitude,
                "longitude": frame.Longitude,
            },
        )
        if "Elevation" in frame:
            ds.coords["elevation"] = frame.Elevation

        n_qc_pass = int(qc_pass.sum())
        logger.info("Frame %s: processed successfully (%d traces, %d pass QC)",
                    frame_id, len(ds.slow_time), n_qc_pass)
        return ds

    except Exception:
        logger.exception("Frame %s: processing failed", frame_id)
        return None
