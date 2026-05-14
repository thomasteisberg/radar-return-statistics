"""Backfill the ``frame_collections`` root attribute on existing icechunk stores.

The viewer needs to know which collection each frame came from. Going forward,
the pipeline writes this automatically (see ``store.update_frame_index``);
this script stamps the attribute onto stores that predate that change.

We query xOPR for every collection listed in the pipeline config and look
up each stored frame_id, so the result is authoritative — no year-based
guessing (multiple seasons in the same calendar year would collide).

Usage:
    uv run python scripts/migrations/backfill_collections_attr.py \\
        --config config/config_greenland.yaml [--dry-run]
"""
from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

import click
import zarr
from xopr import OPRConnection

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
from radar_return_statistics import store as store_mod  # noqa: E402
from radar_return_statistics.config import load_config  # noqa: E402

logger = logging.getLogger("backfill_collections")


def _find_active_pipeline_pids(config_path: Path) -> list[int]:
    target = str(config_path.resolve())
    target_name = config_path.name
    found: list[int] = []
    my_pid = os.getpid()
    for pid_dir in Path("/proc").iterdir():
        if not pid_dir.name.isdigit():
            continue
        pid = int(pid_dir.name)
        if pid == my_pid:
            continue
        try:
            cmdline = (pid_dir / "cmdline").read_bytes().replace(b"\x00", b" ").decode("utf-8", "replace")
        except (FileNotFoundError, PermissionError, ProcessLookupError):
            continue
        if "radar_return_statistics" not in cmdline:
            continue
        if target in cmdline or target_name in cmdline:
            found.append(pid)
    return found


@click.command()
@click.option("--config", "config_path", required=True, type=click.Path(exists=True, path_type=Path))
@click.option("--dry-run", is_flag=True)
@click.option("--force", is_flag=True, help="Run even if an active pipeline is detected.")
def main(config_path: Path, dry_run: bool, force: bool) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

    config = load_config(config_path)
    collections = config["query"].get("collections") or []
    if not collections:
        logger.error("Config has no query.collections to backfill from.")
        sys.exit(2)

    active = _find_active_pipeline_pids(config_path)
    if active and not force:
        logger.error("Active pipeline detected (PIDs %s); pass --force to override.", active)
        sys.exit(2)

    repo = store_mod.open_or_create_repo(config["store"])

    ro = repo.readonly_session("main")
    root_ro = zarr.open_group(ro.store, mode="r")
    frame_names = list(root_ro.attrs.get("frame_names", []) or [])
    if not frame_names:
        logger.error("Store has no frame_names attribute; nothing to backfill.")
        sys.exit(2)

    logger.info("Querying xopr for frames in %d collection(s): %s", len(collections), collections)
    opr = OPRConnection(cache_dir=config["opr"].get("cache_dir"))
    gdf = opr.query_frames(collections=collections, exclude_geometry=True)
    fid_to_collection = dict(zip(gdf.index, gdf["collection"]))
    logger.info("xopr returned %d candidate frames", len(fid_to_collection))

    new_mapping: dict[str, str] = {}
    missing: list[str] = []
    for name in frame_names:
        col = fid_to_collection.get(name)
        if col:
            new_mapping[name] = col
        else:
            missing.append(name)
    if missing:
        logger.warning("%d stored frame(s) had no collection match in xopr: %s%s",
                       len(missing), missing[:5], " ..." if len(missing) > 5 else "")

    existing = list(root_ro.attrs.get("frame_collections", []) or [])
    if existing and len(existing) == len(frame_names):
        existing_map = dict(zip(frame_names, existing))
        unchanged = sum(1 for name, col in new_mapping.items() if existing_map.get(name) == col)
        if unchanged == len(frame_names):
            logger.info("frame_collections already up to date (%d frames).", len(frame_names))
            return

    logger.info("Will write frame_collections for %d frames (%d resolved, %d unresolved).",
                len(frame_names), len(new_mapping), len(missing))
    if dry_run:
        return

    session = repo.writable_session("main")
    store_mod.update_frame_index(session, frame_collections=new_mapping)
    snapshot = session.commit("Backfill frame_collections via xopr lookup")
    logger.info("Done. Snapshot: %s", snapshot)


if __name__ == "__main__":
    main()
