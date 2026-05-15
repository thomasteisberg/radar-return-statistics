import logging
from concurrent.futures import ProcessPoolExecutor, as_completed

import aiohttp
import fsspec
import xarray as xr
from xopr import OPRConnection
from xopr import geometry as xopr_geometry

from . import store
from .config import load_config
from .processing import process_frame

logger = logging.getLogger(__name__)


# fsspec's HTTPFileSystem inherits aiohttp's default ~5min total timeout, but
# the default sock_read timeout (5 min) can fire on slow CReSIS downloads when
# many workers compete for bandwidth on 200 MB+ frames. Set generous totals.
def _configure_fsspec_timeout(total: int = 900) -> None:
    timeout = aiohttp.ClientTimeout(total=total, sock_read=total)
    for proto in ("http", "https"):
        fsspec.config.conf.setdefault(proto, {}).setdefault("client_kwargs", {})["timeout"] = timeout


def _get_region_geometry(region_config: dict):
    """Build a merged region geometry from config.

    If ``subregion`` is specified for antarctic regions, the geometry includes
    all features in that subregion *plus* their associated ice shelves (looked
    up via the ``Asso_Shelf`` column in the MEaSUREs boundaries dataset).
    """
    area = region_config.get("area", "antarctic")
    name = region_config.get("name")
    type_ = region_config.get("type")
    regions = region_config.get("regions")
    subregion = region_config.get("subregion")

    if area == "greenland":
        return xopr_geometry.get_greenland_regions(
            name=name, type=type_, region=regions, subregion=subregion,
            merge_regions=True,
        )

    if subregion is not None:
        return _antarctic_subregion_with_shelves(subregion)

    return xopr_geometry.get_antarctic_regions(
        name=name, type=type_, region=regions, merge_regions=True,
    )


def _antarctic_subregion_with_shelves(subregion):
    """Return merged geometry for an Antarctic subregion including associated shelves."""
    import shapely

    gdf = xopr_geometry.get_antarctic_regions(subregion=subregion, merge_regions=False)
    if gdf is None or len(gdf) == 0:
        raise ValueError(f"No regions found for subregion '{subregion}'")

    # Collect associated shelf names (Asso_Shelf may contain slash-separated compound names)
    shelf_names: set[str] = set()
    for val in gdf["Asso_Shelf"].dropna().unique():
        for part in str(val).split("/"):
            shelf_names.add(part.strip())

    if shelf_names:
        all_antarctic = xopr_geometry.get_antarctic_regions(merge_regions=False)
        shelf_rows = all_antarctic[
            (all_antarctic["NAME"].isin(shelf_names)) & (all_antarctic["TYPE"] == "FL")
        ]
        if len(shelf_rows) > 0:
            import pandas as pd
            gdf = pd.concat([gdf, shelf_rows], ignore_index=True)
            logger.info(
                "Subregion %s: %d grounded regions + %d associated shelves",
                subregion, len(gdf) - len(shelf_rows), len(shelf_rows),
            )

    # Merge in Antarctic polar stereographic, then reproject back
    projected = gdf.to_crs("EPSG:3031")
    merged = shapely.ops.unary_union(projected.geometry)

    area_km2 = merged.area / 1e6
    if area_km2 >= 100000:
        tol = 1000
    elif area_km2 >= 10000:
        tol = 100
    else:
        tol = 0
    if tol > 0:
        merged = shapely.buffer(merged, tol).simplify(tolerance=tol)

    return xopr_geometry.project_geojson(merged, source_crs="EPSG:3031", target_crs="EPSG:4326")


def _process_frame_worker(stac_item_row, config):
    """Worker function for parallel processing. Creates its own OPR connection."""
    _configure_fsspec_timeout()
    opr = OPRConnection(cache_dir=config["opr"].get("cache_dir"))
    return process_frame(opr, stac_item_row, config)


def run(config_path: str | None = None, *, config: dict | None = None, reprocess: bool = False, commit_message: str | None = None) -> None:
    """Main pipeline: query frames, process new ones, store results in icechunk."""
    if config is None:
        config = load_config(config_path)

    _configure_fsspec_timeout()

    # Open or create icechunk repo
    repo = store.open_or_create_repo(config["store"])

    # Get already-processed frames
    if reprocess:
        processed_frames = set()
        logger.info("Reprocess mode: ignoring existing frame tracking")
    else:
        processed_frames = store.get_processed_frames(repo)
        logger.info("Found %d already-processed frames", len(processed_frames))

    # Query frames from OPR
    opr = OPRConnection(cache_dir=config["opr"].get("cache_dir"))
    region_geom = _get_region_geometry(config["region"])

    query_kwargs = {"geometry": region_geom}
    if config["query"].get("collections"):
        query_kwargs["collections"] = config["query"]["collections"]
    if config["query"].get("max_items"):
        query_kwargs["max_items"] = config["query"]["max_items"]

    logger.info("Querying frames from OPR...")
    frames_gdf = opr.query_frames(**query_kwargs)
    logger.info("Found %d total frames", len(frames_gdf))

    # Check for stored frames outside the current query scope
    query_frame_ids = set(frames_gdf.index)
    orphaned_frames = processed_frames - query_frame_ids
    remove_out_of_scope = config["store"].get("remove_out_of_scope", False)
    if orphaned_frames:
        logger.warning(
            "%d stored frames are outside the current query scope%s: %s",
            len(orphaned_frames),
            " — will be removed" if remove_out_of_scope else " (set store.remove_out_of_scope: true to remove)",
            ", ".join(sorted(orphaned_frames)),
        )

    # Filter to unprocessed
    new_frame_ids = query_frame_ids - processed_frames
    nothing_to_process = not new_frame_ids
    nothing_to_remove = not (orphaned_frames and remove_out_of_scope)
    if nothing_to_process and nothing_to_remove:
        logger.info("All frames already processed, nothing to do")
        return

    logger.info("%d frames to process", len(new_frame_ids))

    max_workers = config["processing"].get("max_workers", 4)
    checkpoint_every = config["processing"].get("checkpoint_every") or 0
    total_to_process = len(new_frame_ids)

    # Open initial session; apply pre-write operations so they land in the
    # very first commit (even if it's a checkpoint).
    session = repo.writable_session("main")
    session_dirty = False

    if reprocess:
        store.clear_store(session)
        session_dirty = True

    if orphaned_frames and remove_out_of_scope:
        n_removed = store.remove_frames(session, orphaned_frames)
        logger.info("Removed %d traces from %d out-of-scope frames", n_removed, len(orphaned_frames))
        session_dirty = True

    # Run-wide tallies (across all checkpoints + final commit).
    total_frames_written = 0
    total_traces_written = 0
    all_collections: set[str] = set()

    # Per-checkpoint state — flushed at each checkpoint commit.
    batch_count = 0
    batch_frame_collections: dict[str, str] = {}

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        for fid in new_frame_ids:
            row = frames_gdf.loc[fid]
            future = executor.submit(_process_frame_worker, row, config)
            futures[future] = fid

        for future in as_completed(futures):
            fid = futures[future]
            try:
                ds = future.result()
            except Exception:
                logger.exception("Frame %s raised an exception", fid)
                continue
            if ds is None:
                logger.warning("Frame %s returned no results", fid)
                continue

            store.write_frame_results(session, fid, ds)
            session_dirty = True
            n_traces = len(ds.slow_time)
            total_frames_written += 1
            total_traces_written += n_traces
            batch_count += 1
            if "collection" in ds.attrs:
                col = str(ds.attrs["collection"])
                batch_frame_collections[fid] = col
                all_collections.add(col)
            logger.info("Completed frame %s (%d/%d)", fid, total_frames_written, total_to_process)

            if checkpoint_every and batch_count >= checkpoint_every:
                store.update_frame_index(session, frame_collections=batch_frame_collections or None)
                cp_msg = (
                    f"[checkpoint] {total_frames_written}/{total_to_process} frames "
                    f"({total_traces_written} traces) — "
                    f"{', '.join(sorted(all_collections)) or 'no collection metadata'}"
                )
                store.commit_session(session, cp_msg)
                session = repo.writable_session("main")
                session_dirty = False
                batch_count = 0
                batch_frame_collections = {}

    if total_frames_written == 0 and not session_dirty:
        logger.warning("No frames produced results")
        return

    # Final [run] commit — handles trailing batch + always emits a single
    # entry per run for the viewer (which hides [checkpoint] entries).
    store.update_frame_index(session, frame_collections=batch_frame_collections or None)

    parts = []
    if orphaned_frames and remove_out_of_scope:
        parts.append(f"Removed {len(orphaned_frames)} out-of-scope frames")
    if total_frames_written > 0:
        parts.append(f"Processed {total_frames_written} frames ({total_traces_written} traces)")
    summary = "; ".join(parts) or "no changes"

    if commit_message:
        message = f"[run] {commit_message}\n\n{summary}"
    else:
        collections = config["query"].get("collections")
        if collections and total_frames_written > 0:
            message = f"[run] {', '.join(collections)}: {summary}"
        else:
            message = f"[run] {summary}"
    store.commit_session(session, message)
    logger.info("Done: %s", message)
