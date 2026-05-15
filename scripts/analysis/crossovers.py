"""Find crossover points between radar flight lines and compare measured values."""

import sys
from pathlib import Path

import click
import numpy as np
import zarr

sys.path.insert(0, str(Path(__file__).parents[2] / "src"))
from radar_return_statistics.config import load_config
from radar_return_statistics.store import open_or_create_repo
from radar_return_statistics.crossovers import (
    find_crossovers, make_map, make_scatter, make_differences, make_summary,
    print_summary, HEMISPHERE_PROJ, _hemisphere_for_region,
)

C = 299792458.0
V_ICE = C / np.sqrt(3.17)

VARIABLES = {
    "surface_elevation":       {"label": "Surface Elevation",    "unit": "m"},
    "bed_elevation":           {"label": "Bed Elevation",         "unit": "m"},
    "ice_thickness":           {"label": "Ice Thickness",         "unit": "m"},
    "surface_power_dB":        {"label": "Surface Power",         "unit": "dB"},
    "bed_power_dB":            {"label": "Bed Power",             "unit": "dB"},
    "required_surface_snr_dB": {"label": "Required Surface SNR", "unit": "dB"},
}


def load_data(config):
    repo = open_or_create_repo(config["store"])
    session = repo.readonly_session(branch="main")
    root = zarr.open_group(session.store, mode="r")

    lat = root["latitude"][:]
    lon = root["longitude"][:]
    qc = root["qc_pass"][:]
    surface_twtt = root["surface_twtt"][:]
    bed_twtt = root["bed_twtt"][:]
    surface_elevation = root["surface_elevation"][:]
    bed_elevation = root["bed_elevation"][:]
    surface_power_dB = root["surface_power_dB"][:]
    bed_power_dB = root["bed_power_dB"][:]
    required_surface_snr_dB = root["required_surface_snr_dB"][:]
    frame_index = root["frame_index"][:].astype(np.uint16)
    frame_names = list(root.attrs["frame_names"])

    mask = qc == 1
    idx = np.where(mask)[0]

    ice_thickness = (bed_twtt[mask] - surface_twtt[mask]) * V_ICE / 2

    data = {
        "lat": lat[mask],
        "lon": lon[mask],
        "surface_elevation": surface_elevation[mask],
        "bed_elevation": bed_elevation[mask],
        "ice_thickness": ice_thickness,
        "surface_power_dB": surface_power_dB[mask],
        "bed_power_dB": bed_power_dB[mask],
        "required_surface_snr_dB": required_surface_snr_dB[mask],
        "frame_index": frame_index[mask],
        "orig_idx": idx,
    }
    return data, frame_names


@click.command()
@click.argument("config_path", type=click.Path(exists=True))
@click.option("--threshold", default=1000.0, help="Crossover distance threshold in metres")
@click.option("--output", "output_dir", default="outputs/crossovers", type=click.Path())
@click.option("--verbose", "-v", is_flag=True)
def main(config_path, threshold, output_dir, verbose):
    config = load_config(config_path)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    hemisphere = _hemisphere_for_region(config.get("region", {}))
    crs = HEMISPHERE_PROJ[hemisphere]["epsg"]
    cartopy_proj = HEMISPHERE_PROJ[hemisphere]["cartopy"]()
    click.echo(f"Hemisphere: {hemisphere} ({crs})")

    click.echo("Loading data from store...")
    data, frame_names = load_data(config)
    click.echo(f"Loaded {len(data['lat'])} QC-passing traces across {len(frame_names)} frames")

    click.echo(f"Finding crossovers (threshold={threshold} m)...")
    df, pairs_checked = find_crossovers(data, frame_names, threshold, verbose, VARIABLES, crs=crs)
    click.echo(f"Found {len(df)} crossovers from {pairs_checked} frame pairs checked")

    if df.empty:
        click.echo("No crossovers found — nothing to plot.")
        return

    csv_path = output_dir / "crossovers.csv"
    df.to_csv(csv_path, index=False)
    click.echo(f"Saved: {csv_path}")

    summary = make_summary(df, VARIABLES)
    summary_path = output_dir / "summary.csv"
    summary.to_csv(summary_path, index=False)
    click.echo(f"Saved: {summary_path}")
    click.echo()
    print_summary(summary)
    click.echo()

    click.echo("Generating map.png...")
    make_map(df, output_dir, VARIABLES, crs=crs, cartopy_proj=cartopy_proj)
    click.echo(f"Saved: {output_dir / 'map.png'}")

    click.echo("Generating scatter.png...")
    make_scatter(df, output_dir, VARIABLES)
    click.echo(f"Saved: {output_dir / 'scatter.png'}")

    click.echo("Generating differences.png...")
    make_differences(df, output_dir, VARIABLES)
    click.echo(f"Saved: {output_dir / 'differences.png'}")


if __name__ == "__main__":
    main()
