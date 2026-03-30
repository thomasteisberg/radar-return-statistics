"""Per-frame visualization of radar return statistics.

Usage:
    uv run python -m radar_return_statistics.visualize_frame <config_path> <frame_id> [--output-dir outputs/figures]
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


def load_frame_data(store_config: dict, frame_id: str) -> xr.Dataset:
    """Load data for a single frame from icechunk store, sorted and deduplicated."""
    import icechunk
    storage = make_storage(store_config)
    repo = icechunk.Repository.open(storage=storage)
    session = repo.readonly_session(branch="main")
    root = zarr.open_group(session.store, mode="r")

    all_frame_ids = root["frame_id"][:]
    mask = all_frame_ids == frame_id
    if not mask.any():
        raise ValueError(f"Frame {frame_id} not found in store")

    indices = np.where(mask)[0]

    # Decode slow_time from CF conventions
    st_raw = root["slow_time"][:][indices]
    st_attrs = dict(root["slow_time"].attrs)
    units = st_attrs.get("units", "seconds since 1970-01-01")
    calendar = st_attrs.get("calendar", "proleptic_gregorian")
    slow_time = xr.coding.times.decode_cf_datetime(st_raw, units, calendar)

    data_vars = {
        "surface_twtt": ("trace", root["surface_twtt"][:][indices]),
        "bed_twtt": ("trace", root["bed_twtt"][:][indices]),
        "surface_elevation": ("trace", root["surface_elevation"][:][indices]),
        "bed_elevation": ("trace", root["bed_elevation"][:][indices]),
        "surface_power_dB": ("trace", root["surface_power_dB"][:][indices]),
        "bed_power_dB": ("trace", root["bed_power_dB"][:][indices]),
    }
    if "qc_pass" in root:
        data_vars["qc_pass"] = ("trace", root["qc_pass"][:][indices])

    ds = xr.Dataset(
        data_vars,
        coords={
            "latitude": ("trace", root["latitude"][:][indices]),
            "longitude": ("trace", root["longitude"][:][indices]),
            "slow_time": ("trace", slow_time),
        },
    )

    ds = ds.sortby("slow_time")
    return ds


def _shade_qc_regions(ax, along_track, qc_pass, label_added=False):
    """Shade along-track regions where qc_pass is False."""
    fail = ~qc_pass
    if not fail.any():
        return
    n = len(along_track)
    # Find contiguous runs of QC failure
    diff = np.diff(fail.astype(int), prepend=0, append=0)
    starts = np.where(diff == 1)[0]
    ends = np.where(diff == -1)[0]
    for i, (s, e) in enumerate(zip(starts, ends)):
        # Span from midpoint before first fail to midpoint after last fail
        if s > 0:
            x0 = (along_track[s - 1] + along_track[s]) / 2
        else:
            x0 = along_track[0]
        if e < n:
            x1 = (along_track[e - 1] + along_track[e]) / 2
        else:
            x1 = along_track[-1]
        label = "QC filtered" if i == 0 and not label_added else None
        ax.axvspan(x0, x1, alpha=0.15, color="red", label=label, zorder=0)


def plot_frame(ds: xr.Dataset, frame_id: str, output_path: Path) -> None:
    """Create a 3-panel figure: elevation profile, return power, and map."""
    lat = ds.latitude.values
    lon = ds.longitude.values
    southern = np.nanmean(lat) < 0
    map_crs = ccrs.SouthPolarStereo() if southern else ccrs.NorthPolarStereo()

    # Project coordinates using cartopy (consistent with set_extent)
    pts = map_crs.transform_points(ccrs.PlateCarree(), lon, lat)
    x, y = pts[:, 0], pts[:, 1]

    # Along-track distance in km
    dx = np.diff(x, prepend=x[0])
    dy = np.diff(y, prepend=y[0])
    along_track = np.cumsum(np.sqrt(dx**2 + dy**2)) / 1000.0

    # QC mask
    has_qc = "qc_pass" in ds
    qc_pass = ds.qc_pass.values.astype(bool) if has_qc else np.ones(len(along_track), dtype=bool)

    fig = plt.figure(figsize=(14, 10))
    fig.suptitle(f"Frame: {frame_id}", fontsize=14, fontweight="bold")

    gs = fig.add_gridspec(2, 2, height_ratios=[1, 1], width_ratios=[3, 1.2],
                          hspace=0.3, wspace=0.3)

    # Panel 1: Elevation profile
    ax0 = fig.add_subplot(gs[0, 0])
    ax0.plot(along_track, ds.surface_elevation, label="Surface", color="C0", linewidth=1)
    ax0.plot(along_track, ds.bed_elevation, label="Bed", color="C3", linewidth=1)
    ax0.fill_between(
        along_track, ds.surface_elevation, ds.bed_elevation,
        alpha=0.15, color="C0", label="Ice thickness",
    )
    _shade_qc_regions(ax0, along_track, qc_pass)
    ax0.set_ylabel("WGS84 Elevation [m]")
    ax0.set_xlabel("Along-track distance [km]")
    ax0.legend(loc="upper right", fontsize=9)
    ax0.grid(True, alpha=0.3)

    # Panel 2: Return power
    ax1 = fig.add_subplot(gs[1, 0], sharex=ax0)
    ax1.plot(along_track, ds.surface_power_dB, label="Surface", color="C0", linewidth=1)
    ax1.plot(along_track, ds.bed_power_dB, label="Bed", color="C3", linewidth=1)
    _shade_qc_regions(ax1, along_track, qc_pass, label_added=True)
    ax1.set_ylabel("Peak Power [dB]")
    ax1.set_xlabel("Along-track distance [km]")
    ax1.legend(loc="upper right", fontsize=9)
    ax1.grid(True, alpha=0.3)

    # Panel 3: Map
    ax_map = fig.add_subplot(gs[:, 1], projection=map_crs)

    # Extent with padding
    pad_frac = 0.3
    x_range = x.max() - x.min()
    y_range = y.max() - y.min()
    pad = max(x_range, y_range) * pad_frac
    pad = max(pad, 10_000)  # at least 10 km padding
    ax_map.set_extent(
        [x.min() - pad, x.max() + pad, y.min() - pad, y.max() + pad],
        crs=map_crs,
    )

    ax_map.add_feature(cfeature.LAND, facecolor="#e0e0e0", edgecolor="none")
    ax_map.add_feature(cfeature.OCEAN, facecolor="#c8e1f0", edgecolor="none")
    ax_map.coastlines(resolution="50m", linewidth=0.8, color="#444444")
    ax_map.gridlines(draw_labels=True, linewidth=0.3, alpha=0.5, color="gray")

    # Color track: good traces by along-track, QC-failed in red
    good = qc_pass.astype(bool)
    if good.any():
        sc = ax_map.scatter(
            lon[good], lat[good], c=along_track[good], cmap="viridis", s=4,
            transform=ccrs.PlateCarree(), zorder=5,
        )
        cb = plt.colorbar(sc, ax=ax_map, shrink=0.6, pad=0.08)
        cb.set_label("Along-track [km]", fontsize=9)
    if (~good).any():
        ax_map.scatter(
            lon[~good], lat[~good], c="red", s=4, alpha=0.4,
            transform=ccrs.PlateCarree(), zorder=4, label="QC filtered",
        )

    # Mark start and end
    ax_map.plot(lon[0], lat[0], "o", color="C2", markersize=7, transform=ccrs.PlateCarree(),
                zorder=6, label="Start")
    ax_map.plot(lon[-1], lat[-1], "s", color="C3", markersize=7, transform=ccrs.PlateCarree(),
                zorder=6, label="End")

    ax_map.legend(loc="lower left", fontsize=8)
    ax_map.set_title("Flight track", fontsize=11)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info("Saved figure to %s", output_path)


@click.command()
@click.argument("config_path", type=click.Path(exists=True))
@click.argument("frame_id")
@click.option("--output-dir", default="outputs/figures", help="Output directory for figures")
def main(config_path: str, frame_id: str, output_dir: str) -> None:
    """Generate a per-frame visualization from an icechunk store."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    config = load_config(config_path)
    ds = load_frame_data(config["store"], frame_id)
    output_path = Path(output_dir) / f"{frame_id}.png"
    plot_frame(ds, frame_id, output_path)


if __name__ == "__main__":
    main()
