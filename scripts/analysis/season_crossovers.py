"""Cross-season crossover analysis.

Runs the standard find_crossovers across the whole store, then filters to
pairs where exactly one frame is from the given collection ("just-added
season vs everything already in the store"). Saves the filtered CSV and the
standard plots so we can track how a newly-added season agrees with prior
data.

Usage:
    uv run python scripts/analysis/season_crossovers.py \\
        config/config_greenland.yaml \\
        --collection 2014_Greenland_P3 \\
        [--threshold 1000] \\
        [--output outputs/crossovers_<store>/<collection>]
"""
from __future__ import annotations

import sys
from pathlib import Path

import click
import numpy as np
import zarr

sys.path.insert(0, str(Path(__file__).parents[2] / "src"))
from radar_return_statistics.config import load_config
from radar_return_statistics.store import open_or_create_repo
from radar_return_statistics.crossovers import (
    find_crossovers, make_map, make_scatter, make_differences,
    make_summary, print_summary, HEMISPHERE_PROJ, _hemisphere_for_region,
)

# Mirrors scripts/analysis/crossovers.py
VARIABLES = {
    "surface_elevation":       {"label": "Surface Elevation",    "unit": "m"},
    "bed_elevation":           {"label": "Bed Elevation",         "unit": "m"},
    "ice_thickness":           {"label": "Ice Thickness",         "unit": "m"},
    "surface_power_dB":        {"label": "Surface Power",         "unit": "dB"},
    "bed_power_dB":            {"label": "Bed Power",             "unit": "dB"},
    "required_surface_snr_dB": {"label": "Required Surface SNR", "unit": "dB"},
}


def load_data(config):
    """Same shape as scripts/analysis/crossovers.py:load_data, plus frame_collections."""
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
    frame_collections = list(root.attrs.get("frame_collections", []) or [])

    if not frame_collections:
        raise SystemExit(
            "Store has no frame_collections root attr; run "
            "scripts/migrations/backfill_collections_attr.py first."
        )

    mask = qc == 1
    idx = np.where(mask)[0]
    data = {
        "lat": lat[mask],
        "lon": lon[mask],
        "surface_twtt": surface_twtt[mask],
        "bed_twtt": bed_twtt[mask],
        "surface_elevation": surface_elevation[mask],
        "bed_elevation": bed_elevation[mask],
        "ice_thickness": surface_elevation[mask] - bed_elevation[mask],
        "surface_power_dB": surface_power_dB[mask],
        "bed_power_dB": bed_power_dB[mask],
        "required_surface_snr_dB": required_surface_snr_dB[mask],
        "frame_index": frame_index[mask],
        "orig_idx": idx,
    }
    name_to_collection = dict(zip(frame_names, frame_collections))
    return data, frame_names, name_to_collection


@click.command()
@click.argument("config_path", type=click.Path(exists=True))
@click.option("--collection", required=True, help="The just-added collection to compare against existing data.")
@click.option("--threshold", default=1000.0, help="Crossover distance threshold in metres.")
@click.option("--output", "output_dir", default=None, type=click.Path(),
              help="Output directory (default outputs/crossovers_<store>/<collection>).")
@click.option("--verbose", "-v", is_flag=True)
def main(config_path, collection, threshold, output_dir, verbose):
    config = load_config(config_path)
    hemisphere = _hemisphere_for_region(config.get("region", {}))
    crs = HEMISPHERE_PROJ[hemisphere]["epsg"]
    cartopy_proj = HEMISPHERE_PROJ[hemisphere]["cartopy"]()

    if output_dir is None:
        store_label = (config["store"].get("s3_prefix") or
                       config["store"].get("path") or "store").rsplit("/", 1)[-1]
        output_dir = f"outputs/crossovers_{store_label}/{collection}"
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    click.echo(f"Hemisphere: {hemisphere} ({crs})")
    click.echo("Loading data from store...")
    data, frame_names, name_to_collection = load_data(config)
    click.echo(f"Loaded {len(data['lat'])} QC-passing traces across {len(frame_names)} frames")

    other_frames = [n for n, c in name_to_collection.items() if c != collection]
    target_frames = [n for n, c in name_to_collection.items() if c == collection]
    click.echo(f"Target {collection}: {len(target_frames)} frames; "
               f"other collections: {len(other_frames)} frames")
    if not target_frames:
        raise SystemExit(f"No frames found for collection {collection!r} in this store.")
    if not other_frames:
        raise SystemExit("No frames from other collections to compare against.")

    click.echo(f"Finding crossovers (threshold={threshold} m, full store)...")
    df, pairs_checked = find_crossovers(data, frame_names, threshold, verbose, VARIABLES, crs=crs)
    click.echo(f"Found {len(df)} total crossovers from {pairs_checked} frame pairs checked")

    if df.empty:
        click.echo("No crossovers anywhere — nothing to filter.")
        return

    df["collection_a"] = df["frame_a"].map(name_to_collection)
    df["collection_b"] = df["frame_b"].map(name_to_collection)
    a_is_target = df["collection_a"] == collection
    b_is_target = df["collection_b"] == collection
    cross_season = a_is_target ^ b_is_target  # exactly one side is the new collection
    df = df[cross_season].reset_index(drop=True)
    click.echo(f"Cross-season crossovers (one side = {collection}): {len(df)}")

    if df.empty:
        click.echo("No cross-season crossovers between the new collection and existing data.")
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
