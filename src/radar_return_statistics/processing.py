import logging

import numpy as np
import pandas as pd
import scipy.constants
import xarray as xr
from xopr import OPRConnection

logger = logging.getLogger(__name__)

SURFACE_KEY = "standard:surface"
BED_KEY = "standard:bottom"


def extract_layer_peak_power(radar_ds, layer_twtt, margin_twtt):
    """Extract peak power (dB) and its TWTT within a margin around a layer pick."""
    # Align slow_time coordinates
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

    # Mask to margin around layer pick
    start_twtt = layer_twtt - margin_twtt
    end_twtt = layer_twtt + margin_twtt
    data_within_margin = radar_ds.where(
        (radar_ds.twtt >= start_twtt) & (radar_ds.twtt <= end_twtt),
        drop=True,
    )

    # Peak power in dB
    power_dB = 10 * np.log10(np.abs(data_within_margin.Data))
    peak_twtt_index = power_dB.argmax(dim="twtt")
    peak_twtt = power_dB.twtt[peak_twtt_index]
    peak_power = power_dB.isel(twtt=peak_twtt_index)

    peak_twtt = peak_twtt.drop_vars("twtt")
    peak_power = peak_power.drop_vars("twtt")

    return peak_twtt, peak_power


def _build_qc_mask(frame, qc_config):
    """Build a boolean mask (True = pass QC) along slow_time.

    Uses roll angle if available, otherwise falls back to heading rate.
    Returns None if no QC checks are configured.
    """
    mask = xr.ones_like(frame.slow_time, dtype=bool)
    applied_any = False

    max_roll = qc_config.get("max_roll_deg")
    max_heading_rate = qc_config.get("max_heading_rate_deg_s")

    if max_roll is not None and "Roll" in frame:
        roll_deg = np.rad2deg(frame.Roll)
        roll_ok = np.abs(roll_deg) <= max_roll
        n_fail = int((~roll_ok).sum())
        if n_fail > 0:
            logger.debug("  QC roll: %d/%d traces exceed %.1f deg",
                         n_fail, len(roll_ok), max_roll)
        mask = mask & roll_ok
        applied_any = True
    elif max_heading_rate is not None and "Heading" in frame:
        # Heading rate as fallback when roll is unavailable
        heading_deg = np.rad2deg(frame.Heading)
        # Compute time differences in seconds
        dt = frame.slow_time.diff("slow_time").dt.total_seconds()
        dheading = heading_deg.diff("slow_time")
        # Wrap heading differences to [-180, 180]
        dheading = (dheading + 180) % 360 - 180
        heading_rate = np.abs(dheading / dt)
        heading_ok = heading_rate <= max_heading_rate
        # First trace has no rate — pass it
        heading_ok = xr.concat(
            [xr.DataArray(True, coords={"slow_time": frame.slow_time.values[0]}), heading_ok],
            dim="slow_time",
        )
        n_fail = int((~heading_ok).sum())
        if n_fail > 0:
            logger.debug("  QC heading rate: %d/%d traces exceed %.1f deg/s",
                         n_fail, len(heading_ok), max_heading_rate)
        mask = mask & heading_ok
        applied_any = True

    return mask if applied_any else None


def process_frame(opr: OPRConnection, stac_item, config: dict) -> xr.Dataset | None:
    """Process a single radar frame and return a Dataset of metrics, or None on failure."""
    proc = config["processing"]
    qc_config = config.get("qc", {})
    frame_id = stac_item.name if hasattr(stac_item, "name") else stac_item.get("id", "unknown")

    try:
        # Load frame
        frame = opr.load_frame(stac_item, data_product=proc["data_product"])
        frame = frame.sortby("slow_time")

        # Decimate: pick one trace per interval (no averaging)
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

        # Get layer picks
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

        # QC filtering — build mask but keep all traces
        qc_mask = _build_qc_mask(frame, qc_config)
        if qc_mask is not None:
            n_pass = int(qc_mask.sum())
            n_total = len(qc_mask)
            min_traces = qc_config.get("min_traces_after_qc", 10)
            if n_pass < min_traces:
                logger.warning("Frame %s: only %d/%d traces pass QC (need %d), skipping",
                               frame_id, n_pass, n_total, min_traces)
                return None
            if n_pass < n_total:
                logger.info("Frame %s: QC filtered %d/%d traces", frame_id, n_total - n_pass, n_total)

        # Convert margin from meters to TWTT
        speed_in_ice = scipy.constants.c / np.sqrt(proc["ice_permittivity"])
        margin_twtt = proc["layer_margin_m"] / speed_in_ice

        surface_layer = layers[SURFACE_KEY]
        bed_layer = layers[BED_KEY]

        # Extract surface and bed peak power
        surface_twtt, surface_power = extract_layer_peak_power(
            frame, surface_layer["twtt"], margin_twtt
        )
        bed_twtt, bed_power = extract_layer_peak_power(
            frame, bed_layer["twtt"], margin_twtt
        )

        # Compute WGS84 elevations from frame Elevation and layer TWTT
        # Surface elevation = aircraft_elevation - c/2 * surface_twtt
        # Bed elevation = surface_elevation - v_ice/2 * (bed_twtt - surface_twtt)
        ice_permittivity = proc["ice_permittivity"]
        c = scipy.constants.c
        v_ice = c / np.sqrt(ice_permittivity)

        surface_elevation = frame.Elevation - (c / 2) * surface_twtt
        bed_elevation = surface_elevation - (v_ice / 2) * (bed_twtt - surface_twtt)

        # Build qc_pass flag (default all True if no QC configured)
        if qc_mask is not None:
            qc_pass = qc_mask.rename({"slow_time": "slow_time"})
        else:
            qc_pass = xr.ones_like(frame.slow_time, dtype=bool)

        # Assemble output dataset
        metric_vars = {
            "surface_twtt": surface_twtt,
            "bed_twtt": bed_twtt,
            "surface_elevation": surface_elevation,
            "bed_elevation": bed_elevation,
            "surface_power_dB": surface_power,
            "bed_power_dB": bed_power,
        }

        # NaN out metrics where QC fails
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
