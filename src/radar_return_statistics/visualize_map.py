"""Map visualization of radar return statistics variables.

Usage:
    uv run python -m radar_return_statistics.visualize_map <config_path> [--output-dir outputs/maps]
"""

import logging
from pathlib import Path

import cartopy.crs as ccrs
import cartopy.feature as cfeature
import click
import matplotlib.pyplot as plt
import numpy as np
import zarr
import xarray as xr

from .config import load_config
from .store import make_storage

logger = logging.getLogger(__name__)

VARIABLES = {
    "surface_power_dB": {"label": "Surface Return Power [dB]", "cmap": "viridis"},
    "bed_power_dB": {"label": "Bed Return Power [dB]", "cmap": "viridis"},
    "surface_elevation": {"label": "Surface Elevation [m WGS84]", "cmap": "terrain"},
    "bed_elevation": {"label": "Bed Elevation [m WGS84]", "cmap": "terrain"},
    "surface_twtt": {"label": "Surface TWTT [s]", "cmap": "viridis"},
    "bed_twtt": {"label": "Bed TWTT [s]", "cmap": "viridis"},
}


def load_all_data(store_config: dict) -> xr.Dataset:
    """Load all data from icechunk store."""
    import icechunk
    storage = make_storage(store_config)
    repo = icechunk.Repository.open(storage=storage)
    session = repo.readonly_session(branch="main")
    root = zarr.open_group(session.store, mode="r")

    # Decode slow_time
    st_raw = root["slow_time"][:]
    st_attrs = dict(root["slow_time"].attrs)
    units = st_attrs.get("units", "seconds since 1970-01-01")
    calendar = st_attrs.get("calendar", "proleptic_gregorian")
    slow_time = xr.coding.times.decode_cf_datetime(st_raw, units, calendar)

    data_vars = {}
    for var in VARIABLES:
        if var in root:
            data_vars[var] = ("trace", root[var][:])

    # QC mask
    if "qc_pass" in root:
        qc_pass = root["qc_pass"][:].astype(bool)
    else:
        qc_pass = np.ones(len(slow_time), dtype=bool)

    ds = xr.Dataset(
        data_vars,
        coords={
            "latitude": ("trace", root["latitude"][:]),
            "longitude": ("trace", root["longitude"][:]),
            "slow_time": ("trace", slow_time),
        },
    )

    # Only keep QC-passing traces
    ds = ds.isel(trace=qc_pass)
    return ds


def plot_variable(ds: xr.Dataset, var_name: str, var_info: dict, output_path: Path) -> None:
    """Plot a single variable on a polar stereographic map."""
    lat = ds.latitude.values
    lon = ds.longitude.values
    values = ds[var_name].values

    # Drop NaN
    valid = ~np.isnan(values)
    lat, lon, values = lat[valid], lon[valid], values[valid]
    if len(values) == 0:
        logger.warning("No valid data for %s, skipping", var_name)
        return

    southern = np.nanmean(lat) < 0
    if southern:
        map_crs = ccrs.SouthPolarStereo()
    else:
        map_crs = ccrs.NorthPolarStereo()

    # Project to get extent
    pts = map_crs.transform_points(ccrs.PlateCarree(), lon, lat)
    x, y = pts[:, 0], pts[:, 1]

    fig = plt.figure(figsize=(12, 10))
    ax = fig.add_subplot(1, 1, 1, projection=map_crs)

    # Extent with padding
    pad_frac = 0.1
    x_range = x.max() - x.min()
    y_range = y.max() - y.min()
    pad = max(x_range, y_range) * pad_frac
    pad = max(pad, 20_000)
    ax.set_extent(
        [x.min() - pad, x.max() + pad, y.min() - pad, y.max() + pad],
        crs=map_crs,
    )

    ax.add_feature(cfeature.LAND, facecolor="#e0e0e0", edgecolor="none")
    ax.add_feature(cfeature.OCEAN, facecolor="#c8e1f0", edgecolor="none")
    ax.coastlines(resolution="50m", linewidth=0.8, color="#444444")
    ax.gridlines(draw_labels=True, linewidth=0.3, alpha=0.5, color="gray")

    # Percentile-based color limits to handle outliers
    vmin = np.percentile(values, 2)
    vmax = np.percentile(values, 98)

    sc = ax.scatter(
        lon, lat, c=values, cmap=var_info["cmap"],
        s=2, vmin=vmin, vmax=vmax,
        transform=ccrs.PlateCarree(), zorder=5,
    )
    cb = plt.colorbar(sc, ax=ax, shrink=0.7, pad=0.05)
    cb.set_label(var_info["label"], fontsize=11)

    ax.set_title(var_info["label"], fontsize=14, fontweight="bold")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info("Saved %s", output_path)


@click.command()
@click.argument("config_path", type=click.Path(exists=True))
@click.option("--output-dir", default="outputs/maps", help="Output directory for map figures")
@click.option("--variables", "-v", multiple=True, default=None,
              help="Specific variables to plot (default: all)")
def main(config_path: str, output_dir: str, variables: tuple) -> None:
    """Generate map visualizations for all variables in an icechunk store."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    config = load_config(config_path)
    ds = load_all_data(config["store"])
    logger.info("Loaded %d QC-passing traces", len(ds.trace))

    out = Path(output_dir)
    vars_to_plot = variables if variables else list(VARIABLES.keys())

    for var_name in vars_to_plot:
        if var_name not in VARIABLES:
            logger.warning("Unknown variable %s, skipping", var_name)
            continue
        if var_name not in ds:
            logger.warning("Variable %s not in store, skipping", var_name)
            continue
        output_path = out / f"{var_name}.png"
        plot_variable(ds, var_name, VARIABLES[var_name], output_path)


if __name__ == "__main__":
    main()
