"""Utility to list and process collections by name."""

import logging
from datetime import datetime

import click
import zarr
from xopr import OPRConnection

from . import store as store_mod
from .config import load_config
from .runner import _get_region_geometry, run

logger = logging.getLogger(__name__)


def _get_collection_last_processed(
    repo, frames_by_collection: dict[str, set[str]]
) -> dict[str, datetime]:
    """Walk icechunk history to find when each collection was last processed."""
    last_dates: dict[str, datetime] = {}
    prev_frames: set[str] = set()

    for snap in reversed(list(repo.ancestry(branch="main"))):
        try:
            session = repo.readonly_session(snapshot_id=snap.id)
            root = zarr.open_group(session.store, mode="r")
            current = set(root["processed_frames"][:])
        except Exception:
            current = set()

        added = current - prev_frames
        if added:
            for collection, col_frames in frames_by_collection.items():
                if added & col_frames:
                    last_dates[collection] = snap.written_at
        prev_frames = current

    return last_dates


@click.group(invoke_without_command=True)
@click.argument("config_path", type=click.Path(exists=True))
@click.option("--verbose", "-v", is_flag=True, help="Enable debug logging")
@click.pass_context
def cli(ctx, config_path: str, verbose: bool):
    """Manage collections for radar return statistics processing."""
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    ctx.ensure_object(dict)
    ctx.obj["config"] = load_config(config_path)
    if ctx.invoked_subcommand is None:
        ctx.invoke(list_collections)


@cli.command("list")
@click.pass_context
def list_collections(ctx):
    """List all collections in the configured region with processing status."""
    config = ctx.obj["config"]

    opr = OPRConnection(cache_dir=config["opr"].get("cache_dir"))
    region_geom = _get_region_geometry(config["region"])

    click.echo("Querying frames from OPR...")
    frames_gdf = opr.query_frames(geometry=region_geom)

    # Group frame IDs by collection
    frames_by_collection: dict[str, set[str]] = {}
    for fid, row in frames_gdf.iterrows():
        col = row["collection"]
        frames_by_collection.setdefault(col, set()).add(fid)

    # Get processed frames from store
    repo = store_mod.open_or_create_repo(config["store"])
    processed = store_mod.get_processed_frames(repo)

    # Get last-processed dates (only for collections with processed frames)
    collections_with_processed = {
        col: fids for col, fids in frames_by_collection.items() if fids & processed
    }
    last_dates = _get_collection_last_processed(repo, collections_with_processed)

    # Print table
    name_width = max(len(c) for c in frames_by_collection) if frames_by_collection else 20
    header = f"{'Collection':<{name_width}}  {'Frames':>6}  {'Processed':>12}  Last processed"
    click.echo()
    click.echo(header)
    click.echo("-" * len(header))

    for col in sorted(frames_by_collection):
        total = len(frames_by_collection[col])
        done = len(frames_by_collection[col] & processed)
        date_str = last_dates[col].strftime("%Y-%m-%d %H:%M") if col in last_dates else "—"
        click.echo(f"{col:<{name_width}}  {total:>6}  {done:>4}/{total:<6}  {date_str}")

    click.echo()
    click.echo(f"Total: {len(frames_gdf)} frames, {len(processed)} processed")


@cli.command("process")
@click.argument("collections", nargs=-1, required=True)
@click.option("--reprocess", is_flag=True, help="Reprocess all frames in selected collections")
@click.option("--max-items", type=int, default=None, help="Limit frames per collection")
@click.option("--message", "-m", default=None, help="Custom commit message")
@click.pass_context
def process_collections(ctx, collections: tuple[str, ...], reprocess: bool, max_items: int | None, message: str | None):
    """Process one or more collections by name."""
    config = ctx.obj["config"]
    config["query"]["collections"] = list(collections)
    if max_items is not None:
        config["query"]["max_items"] = max_items
    run(config=config, reprocess=reprocess, commit_message=message)


if __name__ == "__main__":
    cli()
