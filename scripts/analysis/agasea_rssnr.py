"""Extract surface/bed power and RSSNR from AGASEA radargrams using Results layer picks."""

import sys
from pathlib import Path

import click
import numpy as np
import pandas as pd
import xarray as xr
from pyproj import Transformer
from scipy.spatial import cKDTree
import scipy.constants

sys.path.insert(0, str(Path(__file__).parents[2] / "src"))
from radar_return_statistics.processing import peak_power_in_window, compute_rssnr_dB

C = scipy.constants.c
N_ICE = 1.78
V_ICE = C / N_ICE
ICE_PERMITTIVITY = N_ICE ** 2
MARGIN_M = 50.0
HI_LO_OFFSET_DB = 39.0


def _instrument(flight_name):
    return "PASIN" if flight_name.startswith("b") else "HiCARS"


def _radargram_dir(radargram_root, flight_name):
    """Return the radargram directory for a flight, or None if not yet extracted."""
    if _instrument(flight_name) == "PASIN":
        d = radargram_root / "BAS_Radargrams_Final" / flight_name
    elif flight_name.startswith("DRP"):
        d = radargram_root / "UTIG_Radargrams_D_Final" / flight_name
    else:
        d = radargram_root / flight_name
    return d if d.is_dir() else None


def _load_segments(radargram_dir, flight_name, instrument):
    """Load and concatenate all segment NetCDFs for a flight."""
    files = sorted(radargram_dir.glob(f"{flight_name}_*.nc"))
    if not files:
        return None

    lats, lons, elevs, datas_hi, datas_lo = [], [], [], [], []
    fast_time = None

    for f in files:
        ds = xr.open_dataset(f)
        lats.append(ds["latitude"].values)
        lons.append(ds["longitude"].values)
        elevs.append(ds["elevation"].values)
        if fast_time is None:
            fast_time = ds["fast-time"].values
        if instrument == "HiCARS":
            datas_hi.append(ds["data_hi_gain"].values)
            datas_lo.append(ds["data_lo_gain"].values)
        else:
            datas_hi.append(ds["data"].values)
        ds.close()

    lat = np.concatenate(lats)
    lon = np.concatenate(lons)
    elev = np.concatenate(elevs)
    data_hi = np.concatenate(datas_hi, axis=0)
    data_lo = np.concatenate(datas_lo, axis=0) if instrument == "HiCARS" else None
    return lat, lon, elev, fast_time, data_hi, data_lo


def _process_flight(results_path, radargram_root, verbose):
    ds = xr.open_dataset(results_path)
    flight = ds.attrs["Flight Transect"]
    instrument = _instrument(flight)

    rad_dir = _radargram_dir(radargram_root, flight)
    if rad_dir is None:
        ds.close()
        return None, flight, "no_dir"

    radar_height = ds["radar_height"].values
    ice_thickness = ds["ice_thickness"].values
    res_lat = ds["latitude"].values
    res_lon = ds["longitude"].values
    ds.close()

    valid = np.isfinite(radar_height) & np.isfinite(ice_thickness) & (ice_thickness > 0)
    if not valid.any():
        return None, flight, "no_valid_picks"

    segs = _load_segments(rad_dir, flight, instrument)
    if segs is None:
        return None, flight, "no_segments"

    rad_lat, rad_lon, rad_elev, fast_time, data_hi, data_lo = segs

    transformer = Transformer.from_crs("EPSG:4326", "EPSG:3031", always_xy=True)
    rx, ry = transformer.transform(rad_lon, rad_lat)
    tree = cKDTree(np.column_stack([rx, ry]))

    ax, ay = transformer.transform(res_lon[valid], res_lat[valid])
    dists, idx = tree.query(np.column_stack([ax, ay]))

    keep = dists < 500.0
    if not keep.any():
        return None, flight, "no_matches"

    valid_idx = np.where(valid)[0][keep]
    rad_idx = idx[keep]
    dist_m = dists[keep]

    margin_twtt = MARGIN_M / V_ICE
    ft_min, ft_max = fast_time[0], fast_time[-1]

    rows = []
    for vi, ri, dm in zip(valid_idx, rad_idx, dist_m):
        t_surf = 2.0 * radar_height[vi] / C
        t_bed = t_surf + 2.0 * ice_thickness[vi] / V_ICE

        if t_surf < ft_min or t_bed > ft_max or t_bed <= t_surf:
            continue

        trace_hi = data_hi[ri]
        if instrument == "HiCARS":
            trace_lo = data_lo[ri]
            P_surf_dB = peak_power_in_window(trace_lo, fast_time, t_surf, margin_twtt)
            P_bed_hi_dB = peak_power_in_window(trace_hi, fast_time, t_bed, margin_twtt)
            if not np.isfinite(P_surf_dB) or not np.isfinite(P_bed_hi_dB):
                continue
            # Normalise bed to lo-gain scale before geometry correction
            P_bed_dB = P_bed_hi_dB - HI_LO_OFFSET_DB
        else:
            P_surf_dB = peak_power_in_window(trace_hi, fast_time, t_surf, margin_twtt)
            P_bed_dB = peak_power_in_window(trace_hi, fast_time, t_bed, margin_twtt)
            if not np.isfinite(P_surf_dB) or not np.isfinite(P_bed_dB):
                continue

        rssnr = compute_rssnr_dB(P_surf_dB, P_bed_dB, t_surf, t_bed, ICE_PERMITTIVITY)
        if not np.isfinite(rssnr):
            continue

        rows.append({
            "lat": res_lat[vi],
            "lon": res_lon[vi],
            "flight": flight,
            "instrument": instrument,
            "ice_thickness": ice_thickness[vi],
            "surface_power_dB": P_surf_dB,
            "bed_power_dB": P_bed_dB,
            "rssnr": rssnr,
            "match_dist_m": dm,
        })

    if not rows:
        return None, flight, "no_output"

    return pd.DataFrame(rows), flight, "ok"


@click.command()
@click.argument("data_dir", type=click.Path(exists=True))
@click.option("--output", "output_path", default="outputs/agasea_rssnr.csv", type=click.Path())
@click.option("--verbose", "-v", is_flag=True)
def main(data_dir, output_path, verbose):
    data_dir = Path(data_dir)
    results_dir = data_dir / "Results"
    radargram_root = data_dir / "Radargrams"
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    results_files = sorted(results_dir.glob("*.nc"))
    click.echo(f"Found {len(results_files)} Results files")

    all_frames = []
    counts = {"ok": 0, "no_dir": 0, "no_valid_picks": 0, "no_segments": 0,
              "no_matches": 0, "no_output": 0}

    for rf in results_files:
        df, flight, status = _process_flight(rf, radargram_root, verbose)
        counts[status] += 1
        if df is not None:
            all_frames.append(df)
            if verbose:
                click.echo(f"  {flight}: {len(df)} traces")
        elif verbose or status != "no_dir":
            click.echo(f"  {flight}: {status}")

    click.echo(f"\nResults: {counts['ok']} flights processed, "
               f"{counts['no_dir']} awaiting extraction, "
               f"{sum(v for k, v in counts.items() if k not in ('ok', 'no_dir'))} other skipped")

    if not all_frames:
        click.echo("No output produced.")
        return

    out = pd.concat(all_frames, ignore_index=True)
    out.to_csv(output_path, index=False)
    click.echo(f"\nSaved {len(out)} traces to {output_path}")

    for inst in ["HiCARS", "PASIN", "all"]:
        sub = out if inst == "all" else out[out["instrument"] == inst]
        if len(sub) == 0:
            continue
        click.echo(f"\n{inst} (N={len(sub)}):")
        click.echo(f"  RSSNR  median={sub['rssnr'].median():.1f} dB  "
                   f"std={sub['rssnr'].std():.1f} dB  "
                   f"range=[{sub['rssnr'].min():.1f}, {sub['rssnr'].max():.1f}]")
        click.echo(f"  Ice    median={sub['ice_thickness'].median():.0f} m")


if __name__ == "__main__":
    main()
