"""Spatial comparison between AGASEA radargram-derived RSSNR and OPR ASE dataset."""

import re
import sys
from pathlib import Path

import click
import numpy as np
import pandas as pd
import zarr
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from pyproj import Transformer
from scipy.spatial import cKDTree

sys.path.insert(0, str(Path(__file__).parents[2] / "src"))
from radar_return_statistics.config import load_config
from radar_return_statistics.store import open_or_create_repo

C = 299792458.0
V_ICE = C / (3.17 ** 0.5)


def _opr_year(frame_name):
    m = re.search(r"(\d{4})", frame_name)
    return int(m.group(1)) if m else 0


def load_opr(config):
    repo = open_or_create_repo(config["store"])
    session = repo.readonly_session(branch="main")
    root = zarr.open_group(session.store, mode="r")

    qc = root["qc_pass"][:]
    mask = qc == 1
    surface_twtt = root["surface_twtt"][:][mask]
    bed_twtt = root["bed_twtt"][:][mask]
    frame_index = root["frame_index"][:][mask]
    frame_names = list(root.attrs["frame_names"])

    return pd.DataFrame({
        "lat": root["latitude"][:][mask],
        "lon": root["longitude"][:][mask],
        "ice_thickness": (bed_twtt - surface_twtt) * V_ICE / 2,
        "required_surface_snr_dB": root["required_surface_snr_dB"][:][mask],
        "frame_name": [frame_names[i] for i in frame_index],
    })


def match_datasets(agasea_df, opr_df, threshold):
    transformer = Transformer.from_crs("EPSG:4326", "EPSG:3031", always_xy=True)
    ax, ay = transformer.transform(agasea_df["lon"].values, agasea_df["lat"].values)
    ox, oy = transformer.transform(opr_df["lon"].values, opr_df["lat"].values)

    tree = cKDTree(np.column_stack([ox, oy]))
    dists, idx = tree.query(np.column_stack([ax, ay]))
    keep = dists < threshold

    a = agasea_df.iloc[np.where(keep)[0]].reset_index(drop=True)
    o = opr_df.iloc[idx[keep]].reset_index(drop=True)

    pairs = pd.DataFrame({
        "flight": a["flight"],
        "instrument": a["instrument"],
        "opr_frame": o["frame_name"],
        "opr_year": o["frame_name"].apply(_opr_year),
        "distance_m": dists[keep],
        "ice_thickness_agasea": a["ice_thickness"].values,
        "ice_thickness_opr": o["ice_thickness"].values,
        "rssnr_agasea": a["rssnr"].values,
        "rssnr_opr": o["required_surface_snr_dB"].values,
        "lat": (a["lat"].values + o["lat"].values) / 2,
        "lon": (a["lon"].values + o["lon"].values) / 2,
    })
    pairs["ice_thickness_diff"] = pairs["ice_thickness_agasea"] - pairs["ice_thickness_opr"]
    pairs["rssnr_diff"] = pairs["rssnr_agasea"] - pairs["rssnr_opr"]

    # Keep only the closest approach per AGASEA flight / OPR frame pair so that
    # parallel or near-parallel segments don't contribute many correlated matches.
    pairs = (pairs.loc[pairs.groupby(["flight", "opr_frame"])["distance_m"].idxmin()]
             .reset_index(drop=True))
    return pairs


def _summary_stats(pairs, instrument):
    sub = pairs if instrument == "all" else pairs[pairs["instrument"] == instrument]
    rows = []
    for diff_col, label in [("ice_thickness_diff", "ice_thickness"), ("rssnr_diff", "rssnr")]:
        d = sub[diff_col].dropna().values
        if len(d) == 0:
            continue
        rows.append({
            "instrument": instrument, "variable": label, "N": len(d),
            "mean_diff": float(np.mean(d)), "std_diff": float(np.std(d)),
            "rms_diff": float(np.sqrt(np.mean(d ** 2))),
            "median_abs_diff": float(np.median(np.abs(d))),
        })
    return rows


def make_map(pairs, output_dir):
    lons, lats = pairs["lon"].values, pairs["lat"].values
    lat_pad = (lats.max() - lats.min()) * 0.1 + 0.5
    lon_pad = (lons.max() - lons.min()) * 0.1 + 0.5
    extent = [lons.min() - lon_pad, lons.max() + lon_pad,
              lats.min() - lat_pad, lats.max() + lat_pad]

    variables = [("ice_thickness_diff", "ΔIce Thickness (m)"),
                 ("rssnr_diff", "ΔRSSNR (dB)")]
    instruments = ["HiCARS", "PASIN"]
    proj = ccrs.SouthPolarStereo()
    fig = plt.figure(figsize=(14, 10), constrained_layout=True)

    for ri, inst in enumerate(instruments):
        sub = pairs[pairs["instrument"] == inst]
        for ci, (col, label) in enumerate(variables):
            ax = fig.add_subplot(2, 2, ri * 2 + ci + 1, projection=proj)
            ax.set_extent(extent, crs=ccrs.PlateCarree())
            ax.add_feature(cfeature.OCEAN, color="lightblue")
            ax.add_feature(cfeature.LAND, color="#e8e4dc")
            ax.add_feature(cfeature.COASTLINE, linewidth=0.5)
            if len(sub):
                d = sub[col].values
                vmax = np.nanpercentile(np.abs(d), 95)
                sc = ax.scatter(sub["lon"].values, sub["lat"].values, c=d,
                                cmap="RdBu_r", vmin=-vmax, vmax=vmax,
                                s=5, transform=ccrs.PlateCarree())
                plt.colorbar(sc, ax=ax, label=label, shrink=0.7)
            ax.set_title(f"{inst}: {label}")

    fig.savefig(output_dir / "map.png", dpi=150)
    plt.close(fig)


def make_scatter(pairs, output_dir):
    instruments = ["HiCARS", "PASIN"]
    variables = [
        ("ice_thickness_agasea", "ice_thickness_opr", "Ice Thickness", "m"),
        ("rssnr_agasea", "rssnr_opr", "RSSNR", "dB"),
    ]
    cmap = cm.viridis
    norm = plt.Normalize(pairs["distance_m"].min(), pairs["distance_m"].max())

    fig, axes = plt.subplots(len(instruments), len(variables),
                             figsize=(12, 10), constrained_layout=True)

    for ri, inst in enumerate(instruments):
        sub_inst = pairs[pairs["instrument"] == inst]
        for ci, (a_col, o_col, label, unit) in enumerate(variables):
            ax = axes[ri, ci]
            sub = sub_inst.dropna(subset=[a_col, o_col])
            if len(sub):
                lo = min(sub[a_col].min(), sub[o_col].min())
                hi = max(sub[a_col].max(), sub[o_col].max())
                ax.plot([lo, hi], [lo, hi], color="grey", linewidth=0.8, zorder=0)
                sc = ax.scatter(sub[a_col], sub[o_col], c=sub["distance_m"],
                                cmap=cmap, norm=norm, s=12, alpha=0.7)
                diff = sub[a_col].values - sub[o_col].values
                ax.annotate(f"N={len(sub)}\nMean={np.mean(diff):.1f}\nRMS={np.sqrt(np.mean(diff**2)):.1f}",
                            xy=(0.04, 0.96), xycoords="axes fraction", va="top", fontsize=8)
            ax.set_xlabel(f"AGASEA {label} ({unit})", fontsize=9)
            ax.set_ylabel(f"OPR {label} ({unit})", fontsize=9)
            ax.set_title(f"{inst}: {label}")

    sm = cm.ScalarMappable(cmap=cmap, norm=norm)
    sm.set_array([])
    fig.colorbar(sm, ax=axes, label="Match distance (m)", shrink=0.6)
    fig.savefig(output_dir / "scatter.png", dpi=150)
    plt.close(fig)


def make_differences(pairs, output_dir):
    fig, axes = plt.subplots(1, 2, figsize=(12, 5), constrained_layout=True)
    for ax, (col, label, unit) in zip(axes, [
        ("ice_thickness_diff", "ΔIce Thickness (AGASEA − OPR)", "m"),
        ("rssnr_diff", "ΔRSSNR (AGASEA − OPR)", "dB"),
    ]):
        for inst, color in [("HiCARS", "steelblue"), ("PASIN", "darkorange")]:
            d = pairs[pairs["instrument"] == inst][col].dropna().values
            if len(d) == 0:
                continue
            counts, edges = np.histogram(d, bins="auto")
            centers = (edges[:-1] + edges[1:]) / 2
            ax.bar(centers, counts, width=np.diff(edges), alpha=0.6, color=color,
                   label=f"{inst} (N={len(d)}, RMS={np.sqrt(np.mean(d**2)):.1f})")
        ax.set_xlabel(f"{label} ({unit})", fontsize=9)
        ax.set_ylabel("Count", fontsize=9)
        ax.set_title(label)
        ax.legend(fontsize=8)
    fig.savefig(output_dir / "differences.png", dpi=150)
    plt.close(fig)


@click.command()
@click.argument("rssnr_csv", type=click.Path(exists=True))
@click.argument("config_path", type=click.Path(exists=True))
@click.option("--threshold", default=2000.0, help="Spatial match threshold in metres")
@click.option("--output", "output_dir", default="outputs/agasea_comparison", type=click.Path())
@click.option("--verbose", "-v", is_flag=True)
def main(rssnr_csv, config_path, threshold, output_dir, verbose):
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    click.echo("Loading AGASEA radargram RSSNR...")
    agasea_df = pd.read_csv(rssnr_csv)
    click.echo(f"Loaded {len(agasea_df)} AGASEA traces")

    click.echo("Loading OPR ASE store...")
    config = load_config(config_path)
    opr_df = load_opr(config)
    click.echo(f"Loaded {len(opr_df)} OPR QC-passing traces")

    click.echo(f"Matching datasets (threshold={threshold} m)...")
    pairs = match_datasets(agasea_df, opr_df, threshold)
    click.echo(f"Found {len(pairs)} matched pairs")

    if pairs.empty:
        click.echo("No matched pairs — try increasing --threshold.")
        return

    pairs.to_csv(output_dir / "matched_pairs.csv", index=False)
    click.echo(f"Saved: {output_dir / 'matched_pairs.csv'}")

    rows = []
    for inst in ["HiCARS", "PASIN", "all"]:
        rows.extend(_summary_stats(pairs, inst))
    summary = pd.DataFrame(rows)
    summary.to_csv(output_dir / "summary.csv", index=False)
    click.echo(f"Saved: {output_dir / 'summary.csv'}")

    click.echo()
    hdr = f"{'Instrument':<12} {'Variable':<20} {'N':>6} {'Mean':>10} {'Std':>10} {'RMS':>10} {'MedAbs':>10}"
    click.echo(hdr)
    click.echo("-" * len(hdr))
    for _, r in summary.iterrows():
        click.echo(f"{r['instrument']:<12} {r['variable']:<20} {int(r['N']):>6} "
                   f"{r['mean_diff']:>10.3f} {r['std_diff']:>10.3f} "
                   f"{r['rms_diff']:>10.3f} {r['median_abs_diff']:>10.3f}")
    click.echo()

    if verbose:
        click.echo("OPR year distribution:")
        click.echo(pairs["opr_year"].value_counts().sort_index().to_string())
        click.echo()

    click.echo("Generating plots...")
    make_map(pairs, output_dir)
    make_scatter(pairs, output_dir)
    make_differences(pairs, output_dir)
    click.echo(f"Saved: map.png  scatter.png  differences.png")


if __name__ == "__main__":
    main()
