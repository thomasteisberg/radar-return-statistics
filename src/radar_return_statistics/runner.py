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
    opr = OPRConnection(cache_dir=config["opr"].get("cache_dir"))
    return process_frame(opr, stac_item_row, config)


def run(config_path: str | None = None, *, config: dict | None = None, reprocess: bool = False) -> None:
    """Main pipeline: query frames, process new ones, store results in icechunk."""
    if config is None:
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
