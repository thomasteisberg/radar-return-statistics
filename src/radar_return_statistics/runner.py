import logging
from concurrent.futures import ProcessPoolExecutor, as_completed

import xarray as xr
from xopr import OPRConnection
from xopr import geometry as xopr_geometry

from . import store
from .config import load_config
from .processing import process_frame

logger = logging.getLogger(__name__)


def _get_region_geometry(region_config: dict):
    """Build a merged region geometry from config."""
    area = region_config.get("area", "antarctic")
    name = region_config.get("name")
    type_ = region_config.get("type")
    regions = region_config.get("regions")

    if area == "greenland":
        return xopr_geometry.get_greenland_regions(
            name=name, type=type_, region=regions, merge_regions=True
        )
    else:
        return xopr_geometry.get_antarctic_regions(
            name=name, type=type_, region=regions, merge_regions=True
        )


def _process_frame_worker(stac_item_row, config):
    """Worker function for parallel processing. Creates its own OPR connection."""
    opr = OPRConnection(cache_dir=config["opr"].get("cache_dir"))
    return process_frame(opr, stac_item_row, config)


def run(config_path: str, reprocess: bool = False) -> None:
    """Main pipeline: query frames, process new ones, store results in icechunk."""
    config = load_config(config_path)

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

    # Filter to unprocessed
    frame_ids = set(frames_gdf.index)
    new_frame_ids = frame_ids - processed_frames
    if not new_frame_ids:
        logger.info("All frames already processed, nothing to do")
        return

    logger.info("%d frames to process", len(new_frame_ids))

    # Process frames in parallel
    max_workers = config["processing"].get("max_workers", 4)
    results: list[tuple[str, xr.Dataset]] = []

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
                if ds is not None:
                    results.append((fid, ds))
                    logger.info("Completed frame %s (%d/%d)", fid, len(results), len(new_frame_ids))
                else:
                    logger.warning("Frame %s returned no results", fid)
            except Exception:
                logger.exception("Frame %s raised an exception", fid)

    if not results:
        logger.warning("No frames produced results")
        return

    # Write results to icechunk sequentially
    session = repo.writable_session("main")

    if reprocess:
        store.clear_store(session)

    for fid, ds in results:
        store.write_frame_results(session, fid, ds)

    # Commit
    n_frames = len(results)
    n_traces = sum(len(ds.slow_time) for _, ds in results)
    message = f"Processed {n_frames} frames ({n_traces} traces)"
    store.commit_session(session, message)
    logger.info("Done: %s", message)
