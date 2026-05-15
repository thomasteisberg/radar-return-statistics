"""Shared crossover-detection and plotting utilities."""

import numpy as np
import pandas as pd
import click
from pyproj import Transformer
from shapely.geometry import LineString
from shapely.ops import unary_union
from scipy.spatial import cKDTree
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import cartopy.crs as ccrs
import cartopy.feature as cfeature


# Per-region CRS settings used for distance computation and basemap.
# Keys match the values config.region.area accepts (see runner._get_region_geometry).
HEMISPHERE_PROJ = {
    "antarctic": {
        "epsg": "EPSG:3031",
        "cartopy": lambda: ccrs.SouthPolarStereo(),
    },
    "greenland": {
        "epsg": "EPSG:3413",
        "cartopy": lambda: ccrs.NorthPolarStereo(central_longitude=-45),
    },
}


def _hemisphere_for_region(region_config: dict) -> str:
    """Map config['region']['area'] (default 'antarctic') to a HEMISPHERE_PROJ key."""
    area = (region_config or {}).get("area", "antarctic")
    if area not in HEMISPHERE_PROJ:
        raise ValueError(f"Unknown region.area: {area!r}")
    return area


def _bearing(x, y, idx):
    n = len(x)
    if n == 1:
        return 0.0
    i0 = max(0, idx - 1)
    i1 = min(n - 1, idx + 1)
    if i0 == i1:
        i0, i1 = (0, 1) if idx == 0 else (n - 2, n - 1)
    dx = x[i1] - x[i0]
    dy = y[i1] - y[i0]
    return np.degrees(np.arctan2(dx, dy))


def _acute_angle(b1, b2):
    diff = abs(b1 - b2) % 180.0
    return min(diff, 180.0 - diff)


def _components(geom):
    if geom.geom_type == "Polygon":
        yield geom
    elif geom.geom_type in ("MultiPolygon", "GeometryCollection"):
        for g in geom.geoms:
            if g.geom_type == "Polygon":
                yield g
            elif g.geom_type in ("MultiPolygon", "GeometryCollection"):
                yield from _components(g)


def _clean(df, var):
    return df.dropna(subset=[f"{var}_a", f"{var}_b", f"{var}_diff"])


def find_crossovers(data, frame_names, threshold, verbose, variables, crs="EPSG:3031"):
    """Find crossover points between flight lines.

    data must contain 'lat', 'lon', 'frame_index', and one key per variable in variables.
    ``crs`` selects the projection used for metric distance calculations
    (EPSG:3031 in the south, EPSG:3413 in the north). Returns (DataFrame, pairs_checked).
    """
    transformer = Transformer.from_crs("EPSG:4326", crs, always_xy=True)
    x_all, y_all = transformer.transform(data["lon"], data["lat"])
    fi = data["frame_index"]
    n_frames = len(frame_names)

    frame_local = {}
    for f in range(n_frames):
        local = np.where(fi == f)[0]
        if len(local) >= 2:
            frame_local[f] = local

    frames_with_data = sorted(frame_local.keys())
    n_valid = len(frames_with_data)

    lines = {}
    for f in frames_with_data:
        locs = frame_local[f]
        coords = list(zip(x_all[locs], y_all[locs]))
        lines[f] = LineString(coords)

    pairs_checked = 0
    crossovers = []
    half = threshold / 2.0

    for ii, fi_idx in enumerate(frames_with_data):
        buf_i = lines[fi_idx].buffer(half)
        for jj in range(ii + 1, n_valid):
            fj_idx = frames_with_data[jj]
            pairs_checked += 1

            buf_j = lines[fj_idx].buffer(half)
            intersection = buf_i.intersection(buf_j)
            if intersection.is_empty:
                continue

            merged = unary_union(intersection)
            locs_i = frame_local[fi_idx]
            locs_j = frame_local[fj_idx]
            xi, yi = x_all[locs_i], y_all[locs_i]
            xj, yj = x_all[locs_j], y_all[locs_j]

            for comp in _components(merged):
                minx, miny, maxx, maxy = comp.bounds

                mask_i = (xi >= minx) & (xi <= maxx) & (yi >= miny) & (yi <= maxy)
                mask_j = (xj >= minx) & (xj <= maxx) & (yj >= miny) & (yj <= maxy)
                if not mask_i.any() or not mask_j.any():
                    continue

                sub_i = np.where(mask_i)[0]
                sub_j = np.where(mask_j)[0]
                pts_i = np.column_stack([xi[sub_i], yi[sub_i]])
                pts_j = np.column_stack([xj[sub_j], yj[sub_j]])

                tree_j = cKDTree(pts_j)
                dists, nn_j = tree_j.query(pts_i)
                best_i_local = int(np.argmin(dists))
                best_j_local = int(nn_j[best_i_local])
                min_dist = float(dists[best_i_local])

                if min_dist > threshold:
                    continue

                i_loc = sub_i[best_i_local]
                j_loc = sub_j[best_j_local]

                b_i = _bearing(xi, yi, i_loc)
                b_j = _bearing(xj, yj, j_loc)
                angle = _acute_angle(b_i, b_j)

                if angle < 20.0:
                    continue

                cx = (xi[i_loc] + xj[j_loc]) / 2.0
                cy = (yi[i_loc] + yj[j_loc]) / 2.0

                gi = locs_i[i_loc]
                gj = locs_j[j_loc]

                row = {
                    "frame_a": frame_names[fi_idx],
                    "frame_b": frame_names[fj_idx],
                    "distance_m": min_dist,
                    "angle_deg": angle,
                    "x_proj": cx,
                    "y_proj": cy,
                }
                for var in variables:
                    row[f"{var}_a"] = float(data[var][gi])
                    row[f"{var}_b"] = float(data[var][gj])
                    row[f"{var}_diff"] = row[f"{var}_a"] - row[f"{var}_b"]

                crossovers.append(row)

    if verbose:
        click.echo(f"Frame pairs checked: {pairs_checked}, crossovers found: {len(crossovers)}")

    return pd.DataFrame(crossovers), pairs_checked


def make_map(df, output_dir, variables, crs="EPSG:3031", cartopy_proj=None):
    transformer_inv = Transformer.from_crs(crs, "EPSG:4326", always_xy=True)
    all_lons, all_lats = transformer_inv.transform(df["x_proj"].values, df["y_proj"].values)
    df = df.copy()
    df["_lon"] = all_lons
    df["_lat"] = all_lats

    lat_pad = (all_lats.max() - all_lats.min()) * 0.1 + 0.5
    lon_pad = (all_lons.max() - all_lons.min()) * 0.1 + 0.5
    extent = [all_lons.min() - lon_pad, all_lons.max() + lon_pad,
              all_lats.min() - lat_pad, all_lats.max() + lat_pad]

    n = len(variables)
    ncols = min(3, n)
    nrows = (n + ncols - 1) // ncols
    fig = plt.figure(figsize=(6 * ncols, 5 * nrows), constrained_layout=True)
    proj = cartopy_proj if cartopy_proj is not None else ccrs.SouthPolarStereo()

    for i, var in enumerate(variables):
        ax = fig.add_subplot(nrows, ncols, i + 1, projection=proj)
        ax.set_extent(extent, crs=ccrs.PlateCarree())
        ax.add_feature(cfeature.OCEAN, color="lightblue")
        ax.add_feature(cfeature.LAND, color="#e8e4dc")
        ax.add_feature(cfeature.COASTLINE, linewidth=0.5)

        dfc = _clean(df, var)
        sc = ax.scatter(
            dfc["_lon"].values, dfc["_lat"].values,
            c=dfc[f"{var}_diff"].abs().values,
            cmap="viridis", s=20, transform=ccrs.PlateCarree(),
        )
        meta = variables[var]
        plt.colorbar(sc, ax=ax, label=f"|{meta['label']} diff| ({meta['unit']})", shrink=0.7)
        ax.set_title(meta["label"])

    fig.savefig(output_dir / "map.png", dpi=150)
    plt.close(fig)


def make_scatter(df, output_dir, variables):
    n = len(variables)
    ncols = min(3, n)
    nrows = (n + ncols - 1) // ncols
    fig = plt.figure(figsize=(6 * ncols, 5 * nrows), constrained_layout=True)
    varlist = list(variables.keys())
    norm = plt.Normalize(vmin=20, vmax=90)
    cmap = cm.plasma

    for i, var in enumerate(varlist):
        ax = fig.add_subplot(nrows, ncols, i + 1)
        dfc = _clean(df, var)
        meta = variables[var]

        a_vals = dfc[f"{var}_a"].values
        b_vals = dfc[f"{var}_b"].values
        angles = dfc["angle_deg"].values

        lo = min(a_vals.min(), b_vals.min())
        hi = max(a_vals.max(), b_vals.max())
        ax.plot([lo, hi], [lo, hi], color="grey", linewidth=0.8, zorder=0)

        sc = ax.scatter(a_vals, b_vals, c=angles, cmap=cmap, norm=norm, s=15, alpha=0.7)

        count = len(dfc)
        rms = float(np.sqrt(np.mean(dfc[f"{var}_diff"].values ** 2)))
        ax.annotate(f"N={count}\nRMS={rms:.2f}", xy=(0.04, 0.94), xycoords="axes fraction",
                    va="top", fontsize=8)
        ax.set_xlabel(f"{meta['label']} A ({meta['unit']})", fontsize=8)
        ax.set_ylabel(f"{meta['label']} B ({meta['unit']})", fontsize=8)
        ax.set_title(meta["label"])

    sm = cm.ScalarMappable(cmap=cmap, norm=norm)
    sm.set_array([])
    fig.colorbar(sm, ax=fig.axes, label="Crossing angle (°)", shrink=0.6, pad=0.02)

    fig.savefig(output_dir / "scatter.png", dpi=150)
    plt.close(fig)


def make_differences(df, output_dir, variables):
    n = len(variables)
    ncols = min(3, n)
    nrows = (n + ncols - 1) // ncols
    fig = plt.figure(figsize=(6 * ncols, 5 * nrows), constrained_layout=True)
    varlist = list(variables.keys())

    for i, var in enumerate(varlist):
        ax = fig.add_subplot(nrows, ncols, i + 1)
        dfc = _clean(df, var)
        meta = variables[var]
        diffs = dfc[f"{var}_diff"].values

        counts, edges = np.histogram(diffs, bins="auto")
        centers = (edges[:-1] + edges[1:]) / 2
        ax.bar(centers, counts, width=np.diff(edges), align="center", alpha=0.8)

        mean = float(np.mean(diffs))
        std = float(np.std(diffs))
        rms = float(np.sqrt(np.mean(diffs ** 2)))
        ax.annotate(f"mean={mean:.2f}\nstd={std:.2f}\nRMS={rms:.2f}",
                    xy=(0.97, 0.97), xycoords="axes fraction",
                    ha="right", va="top", fontsize=8)
        ax.set_xlabel(f"A − B ({meta['unit']})", fontsize=8)
        ax.set_ylabel("Count", fontsize=8)
        ax.set_title(meta["label"])

    fig.savefig(output_dir / "differences.png", dpi=150)
    plt.close(fig)


def make_summary(df, variables):
    rows = []
    for var in variables:
        dfc = _clean(df, var)
        diffs = dfc[f"{var}_diff"].values
        rows.append({
            "variable": var,
            "N": len(dfc),
            "mean_diff": float(np.mean(diffs)),
            "std_diff": float(np.std(diffs)),
            "rms_diff": float(np.sqrt(np.mean(diffs ** 2))),
            "median_abs_diff": float(np.median(np.abs(diffs))),
        })
    return pd.DataFrame(rows)


def print_summary(summary):
    hdr = f"{'Variable':<30} {'N':>6} {'Mean':>10} {'Std':>10} {'RMS':>10} {'MedAbs':>10}"
    click.echo(hdr)
    click.echo("-" * len(hdr))
    for _, row in summary.iterrows():
        click.echo(
            f"{row['variable']:<30} {int(row['N']):>6} "
            f"{row['mean_diff']:>10.3f} {row['std_diff']:>10.3f} "
            f"{row['rms_diff']:>10.3f} {row['median_abs_diff']:>10.3f}"
        )
