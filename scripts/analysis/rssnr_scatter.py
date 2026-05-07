"""Scatter plots: RSSNR vs ice thickness and vs signed distance from grounding line.

Signed GL distance convention: positive = grounded ice, negative = floating ice shelf.

Usage:
    uv run python scripts/analysis/rssnr_scatter.py config/config.yaml \\
        [--agasea outputs/agasea_rssnr.csv] \\
        [--output outputs/rssnr_scatter]
"""

import sys
from pathlib import Path

import click
import geopandas as gpd
import matplotlib
matplotlib.use("Agg")
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import pandas as pd
import zarr
from pyproj import Transformer
from shapely.geometry import MultiPoint, Point
from shapely.strtree import STRtree
import scipy.stats

sys.path.insert(0, str(Path(__file__).parents[2] / "src"))
from radar_return_statistics.config import load_config
from radar_return_statistics.store import open_or_create_repo
import xopr.geometry as xg

C = 299792458.0
V_ICE = C / (3.17 ** 0.5)


def _get_grounding_line():
    """Return GR union polygon and its boundary (grounding line) in EPSG:3031."""
    click.echo("Loading MEaSUREs grounding line via xopr.geometry...")
    gr_gdf = xg.get_antarctic_regions(type="GR", merge_regions=False).to_crs("EPSG:3031")
    gr_union = gr_gdf.union_all()
    # Boundary of the grounded ice polygon = grounding line + coastline
    gl_boundary = gr_union.boundary
    return gr_union, gl_boundary


def _signed_distance(lats, lons, gr_union, gl_boundary):
    """Signed distance (m) to grounding line. Positive = grounded, negative = floating."""
    t = Transformer.from_crs("EPSG:4326", "EPSG:3031", always_xy=True)
    xs, ys = t.transform(lons, lats)

    # Build STRtree from boundary line segments for fast nearest-point distance
    if gl_boundary.geom_type == "MultiLineString":
        segments = list(gl_boundary.geoms)
    else:
        segments = [gl_boundary]
    tree = STRtree(segments)

    points = [Point(x, y) for x, y in zip(xs, ys)]
    # Distance to nearest segment
    nearest_idx, dists = tree.query_nearest(points, return_distance=True)
    dists = np.asarray(dists, dtype=float)

    # Sign: grounded (inside GR polygon) → positive, floating/ocean → negative
    inside = np.array([gr_union.contains(p) for p in points], dtype=bool)
    return np.where(inside, dists, -dists)


def load_opr(config):
    repo = open_or_create_repo(config["store"])
    session = repo.readonly_session(branch="main")
    root = zarr.open_group(session.store, mode="r")

    qc = root["qc_pass"][:]
    mask = qc == 1
    surface_twtt = root["surface_twtt"][:][mask]
    bed_twtt = root["bed_twtt"][:][mask]

    return pd.DataFrame({
        "lat": root["latitude"][:][mask],
        "lon": root["longitude"][:][mask],
        "ice_thickness": (bed_twtt - surface_twtt) * V_ICE / 2,
        "rssnr": root["required_surface_snr_dB"][:][mask],
        "source": "OPR",
    })


def _annotate_corr(ax, x, y, color, label, row):
    """Add Pearson r and Spearman rho at a stacked vertical position (row=0 is top)."""
    mask = np.isfinite(x) & np.isfinite(y)
    xc, yc = x[mask], y[mask]
    if len(xc) < 10:
        return
    r, _ = scipy.stats.pearsonr(xc, yc)
    rho, _ = scipy.stats.spearmanr(xc, yc)
    y_pos = 0.97 - row * 0.08
    ax.annotate(f"{label}: r={r:.2f}, ρ={rho:.2f}",
                xy=(0.04, y_pos), xycoords="axes fraction",
                va="top", fontsize=8, color=color,
                bbox=dict(boxstyle="round,pad=0.15", fc="white", ec="none", alpha=0.6))


def _scatter_panel(ax, datasets, x_col, y_col, xlabel, ylabel, title,
                   xlim=None, x_filter_m=None):
    """Draw one scatter panel. x_filter_m excludes |gl_dist_km| < x_filter_m/1000."""
    alpha = 0.15
    s = 6
    for row, (label, df, color) in enumerate(datasets):
        if x_col not in df.columns or y_col not in df.columns:
            continue
        sub = df
        if x_filter_m is not None and "gl_dist_km" in df.columns:
            sub = df[df["gl_dist_km"] >= x_filter_m / 1000.0]
        ax.scatter(sub[x_col], sub[y_col], s=s, alpha=alpha,
                   color=color, label=f"{label} (N={len(sub):,})", rasterized=True)
        _annotate_corr(ax, sub[x_col].values, sub[y_col].values, color, label, row)
    if xlim is not None:
        ax.set_xlim(xlim)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.legend(fontsize=8, markerscale=2)


def make_plots(opr_df, agasea_df, output_dir):
    fig, axes = plt.subplots(2, 2, figsize=(14, 12), constrained_layout=True)

    opr_color = "#2166ac"
    agasea_color = "#d6604d"

    datasets = [("OPR", opr_df, opr_color)]
    if agasea_df is not None:
        datasets.append(("HiCARS (AGASEA)", agasea_df, agasea_color))

    gl_xlabel = "Distance from Grounding Line (km)\n← Ice shelf    |    Grounded ice →"

    # Row 0: original plots
    _scatter_panel(axes[0, 0], datasets,
                   "ice_thickness", "rssnr",
                   "Ice Thickness (m)", "RSSNR (dB)",
                   "RSSNR vs Ice Thickness")

    _scatter_panel(axes[0, 1], datasets,
                   "gl_dist_km", "rssnr",
                   gl_xlabel, "RSSNR (dB)",
                   "RSSNR vs Distance from Grounding Line")
    axes[0, 1].axvline(0, color="k", linewidth=0.8, linestyle="--")

    # Row 1: filtered / zoomed variants
    _scatter_panel(axes[1, 0], datasets,
                   "ice_thickness", "rssnr",
                   "Ice Thickness (m)", "RSSNR (dB)",
                   "RSSNR vs Ice Thickness\n(excluding |GL dist| < 100 m)",
                   x_filter_m=100)

    _scatter_panel(axes[1, 1], datasets,
                   "gl_dist_km", "rssnr",
                   gl_xlabel, "RSSNR (dB)",
                   "RSSNR vs Distance from Grounding Line\n(±100 km)",
                   xlim=(-100, 100))
    axes[1, 1].axvline(0, color="k", linewidth=0.8, linestyle="--")

    out = output_dir / "rssnr_scatter.png"
    fig.savefig(out, dpi=150)
    plt.close(fig)
    click.echo(f"Saved: {out}")


def _kde_panel(ax_main, ax_top, ax_right, datasets, x_col, y_col, xlabel, ylabel, title,
               xlim=None, gl_min_km=None):
    """Draw one KDE panel with 1-D marginal histograms on top and right."""
    handles = []
    for label, df, color in datasets:
        if x_col not in df.columns or y_col not in df.columns:
            continue
        sub = df
        if gl_min_km is not None and "gl_dist_km" in df.columns:
            sub = df[df["gl_dist_km"] >= gl_min_km]
        valid = sub[[x_col, y_col]].dropna()
        if len(valid) < 2:
            continue
        x_vals = valid[x_col]
        y_vals = valid[y_col]

        sns.kdeplot(x=x_vals, y=y_vals, ax=ax_main, color=color,
                    fill=True, alpha=0.25, levels=6)
        sns.kdeplot(x=x_vals, y=y_vals, ax=ax_main, color=color,
                    fill=False, alpha=0.85, levels=6, linewidths=1.0)

        ax_top.hist(x_vals, bins=60, color=color, alpha=0.5, density=True)
        ax_right.hist(y_vals, bins=60, color=color, alpha=0.5, density=True,
                      orientation="horizontal")

        handles.append(mpatches.Patch(color=color, label=f"{label} (N={len(sub):,})"))

    if xlim is not None:
        ax_main.set_xlim(xlim)

    ax_main.set_xlabel(xlabel)
    ax_main.set_ylabel(ylabel)
    ax_top.set_title(title)

    ax_top.tick_params(axis="x", labelbottom=False)
    ax_top.set_ylabel("density", fontsize=7)
    ax_right.tick_params(axis="y", labelleft=False)
    ax_right.set_xlabel("density", fontsize=7)

    if handles:
        ax_main.legend(handles=handles, fontsize=8)


def make_kde_plots(opr_df, agasea_df, output_dir):
    opr_color = "#2166ac"
    agasea_color = "#d6604d"

    datasets = [("OPR", opr_df, opr_color)]
    if agasea_df is not None:
        datasets.append(("HiCARS (AGASEA)", agasea_df, agasea_color))

    gl_xlabel = "Distance from Grounding Line (km)\n← Ice shelf    |    Grounded ice →"

    panels = [
        (0, 0, "ice_thickness", "rssnr", "Ice Thickness (m)", "RSSNR (dB)",
         "RSSNR vs Ice Thickness", None, None, False),
        (0, 1, "gl_dist_km", "rssnr", gl_xlabel, "RSSNR (dB)",
         "RSSNR vs Distance from Grounding Line", None, None, True),
        (1, 0, "ice_thickness", "rssnr", "Ice Thickness (m)", "RSSNR (dB)",
         "RSSNR vs Ice Thickness\n(GL dist > 100 m, grounded only)", None, 0.1, False),
        (1, 1, "gl_dist_km", "rssnr", gl_xlabel, "RSSNR (dB)",
         "RSSNR vs Distance from Grounding Line\n(±100 km)", (-100, 100), None, True),
    ]

    fig = plt.figure(figsize=(16, 14))
    outer = fig.add_gridspec(2, 2, hspace=0.5, wspace=0.5)

    for pr, pc, x_col, y_col, xlabel, ylabel, title, xlim, gl_min_km, vline in panels:
        inner = outer[pr, pc].subgridspec(
            2, 2, height_ratios=[1, 4], width_ratios=[4, 1], hspace=0.05, wspace=0.05
        )
        ax_main = fig.add_subplot(inner[1, 0])
        ax_top = fig.add_subplot(inner[0, 0], sharex=ax_main)
        ax_right = fig.add_subplot(inner[1, 1], sharey=ax_main)

        _kde_panel(ax_main, ax_top, ax_right, datasets,
                   x_col, y_col, xlabel, ylabel, title,
                   xlim=xlim, gl_min_km=gl_min_km)

        if vline:
            ax_main.axvline(0, color="k", linewidth=0.8, linestyle="--")

    out = output_dir / "rssnr_kde.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    click.echo(f"Saved: {out}")


@click.command()
@click.argument("config_path", type=click.Path(exists=True))
@click.option("--agasea", "agasea_csv", default=None, type=click.Path(exists=True),
              help="Path to agasea_rssnr.csv (HiCARS data will be plotted alongside OPR)")
@click.option("--output", "output_dir", default="outputs/rssnr_scatter", type=click.Path())
@click.option("--max-points", default=50000, show_default=True,
              help="Max points per dataset to plot (random subsample if exceeded)")
@click.option("--verbose", "-v", is_flag=True)
def main(config_path, agasea_csv, output_dir, max_points, verbose):
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    click.echo("Loading OPR data...")
    config = load_config(config_path)
    opr_df = load_opr(config)
    click.echo(f"  {len(opr_df):,} QC-passing OPR traces")

    agasea_df = None
    if agasea_csv:
        click.echo("Loading AGASEA HiCARS data...")
        raw = pd.read_csv(agasea_csv)
        agasea_df = raw[raw["instrument"] == "HiCARS"].copy()
        click.echo(f"  {len(agasea_df):,} HiCARS traces")

    # Subsample for plotting
    rng = np.random.default_rng(42)
    if len(opr_df) > max_points:
        opr_df = opr_df.sample(max_points, random_state=42).reset_index(drop=True)
    if agasea_df is not None and len(agasea_df) > max_points:
        idx = rng.choice(len(agasea_df), max_points, replace=False)
        agasea_df = agasea_df.iloc[idx].reset_index(drop=True)

    # Grounding line distances
    gr_union, gl_boundary = _get_grounding_line()

    click.echo(f"Computing GL distances for {len(opr_df):,} OPR traces...")
    opr_df["gl_dist_km"] = _signed_distance(
        opr_df["lat"].values, opr_df["lon"].values, gr_union, gl_boundary
    ) / 1e3

    if agasea_df is not None:
        click.echo(f"Computing GL distances for {len(agasea_df):,} HiCARS traces...")
        agasea_df["gl_dist_km"] = _signed_distance(
            agasea_df["lat"].values, agasea_df["lon"].values, gr_union, gl_boundary
        ) / 1e3

    if verbose:
        for label, df in [("OPR", opr_df)] + ([("HiCARS", agasea_df)] if agasea_df is not None else []):
            click.echo(f"\n{label}:")
            click.echo(f"  RSSNR:   median={df['rssnr'].median():.1f}  std={df['rssnr'].std():.1f} dB")
            click.echo(f"  Ice:     median={df['ice_thickness'].median():.0f} m")
            click.echo(f"  GL dist: median={df['gl_dist_km'].median():.1f} km  "
                       f"floating={( df['gl_dist_km'] < 0).mean()*100:.1f}%")

    make_plots(opr_df, agasea_df, output_dir)
    make_kde_plots(opr_df, agasea_df, output_dir)


if __name__ == "__main__":
    main()
