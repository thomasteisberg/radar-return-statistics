import logging

import numpy as np
import icechunk
import xarray as xr
import zarr
from xarray.coding.times import encode_cf_datetime

logger = logging.getLogger(__name__)

# Per-trace zarr arrays are appended to over the lifetime of the store. xarray's
# default chunking on first write is the length of the first frame (~38 traces),
# which makes the viewer fire thousands of HTTP requests per variable. Force a
# sensible chunk size up front. Existing stores can be migrated with
# scripts/migrations/rechunk_per_trace_arrays.py.
PER_TRACE_CHUNK_SIZE = 10000


def make_storage(store_config: dict) -> icechunk.Storage:
    """Create icechunk Storage from config (local or S3)."""
    backend = store_config.get("backend", "local")
    if backend == "s3":
        return icechunk.s3_storage(
            bucket=store_config["s3_bucket"],
            prefix=store_config.get("s3_prefix"),
            region=store_config.get("s3_region"),
            from_env=True,
        )
    else:
        return icechunk.local_filesystem_storage(str(store_config["path"]))


def open_or_create_repo(store_config: dict) -> icechunk.Repository:
    """Open an existing icechunk repo or create a new one."""
    storage = make_storage(store_config)
    try:
        repo = icechunk.Repository.open(storage=storage)
        logger.info("Opened existing icechunk repo")
    except Exception:
        repo = icechunk.Repository.create(storage=storage)
        logger.info("Created new icechunk repo")
    return repo


def get_processed_frames(repo: icechunk.Repository) -> set[str]:
    """Get set of already-processed frame IDs from the store."""
    try:
        session = repo.readonly_session(branch="main")
        store = session.store
        root = zarr.open_group(store, mode="r")
        if "processed_frames" in root:
            return set(root["processed_frames"][:].tolist())
    except Exception:
        pass
    return set()


def _zarr_append(root: zarr.Group, ds: xr.Dataset) -> None:
    """Append dataset to existing zarr group using zarr directly.

    Bypasses xarray's to_zarr(append_dim=...) which fails when the existing
    store has CF-time-encoded slow_time that xarray can't decode.
    """
    n_new = len(ds.slow_time)

    # Collect all slow_time-dimensioned arrays
    arrays: dict[str, np.ndarray] = {}
    for name in ds.data_vars:
        if ds[name].dims == ("slow_time",):
            arrays[name] = ds[name].values
    for name in ds.coords:
        if name != "slow_time" and hasattr(ds.coords[name], "dims") and ds.coords[name].dims == ("slow_time",):
            arrays[name] = ds.coords[name].values

    # Encode slow_time to match the existing store's CF encoding
    st_arr = root["slow_time"]
    units = st_arr.attrs.get("units", "seconds since 1970-01-01")
    calendar = st_arr.attrs.get("calendar", "proleptic_gregorian")
    encoded_times, _, _ = encode_cf_datetime(ds.slow_time.values, units=units, calendar=calendar)
    arrays["slow_time"] = np.asarray(encoded_times)

    for name, data in arrays.items():
        if name in root:
            arr = root[name]
            old_size = arr.shape[0]
            arr.resize(old_size + n_new)
            arr[old_size:] = data
        # Variables present in new frames but not yet in store (e.g. required_surface_snr_dB)
        # are silently skipped — they won't appear until a --reprocess run.


def write_frame_results(
    session: icechunk.Session,
    frame_id: str,
    results_ds: xr.Dataset,
) -> None:
    """Write frame results to icechunk store, appending along slow_time dimension."""
    store = session.store
    root = zarr.open_group(store, mode="a")
    first_write = "surface_twtt" not in root

    if first_write:
        encoding = {
            name: {"chunks": (PER_TRACE_CHUNK_SIZE,)}
            for name in (*results_ds.data_vars, *results_ds.coords)
            if "slow_time" in results_ds[name].dims
        }
        results_ds.to_zarr(store, mode="w", encoding=encoding)
    else:
        _zarr_append(root, results_ds)

    # Track processed frame
    if "processed_frames" not in root:
        root.create_array(
            "processed_frames",
            data=np.array([frame_id], dtype="U100"),
            chunks=(1000,),
        )
    else:
        existing = root["processed_frames"]
        new_size = existing.shape[0] + 1
        existing.resize(new_size)
        existing[new_size - 1] = frame_id


def remove_frames(session: icechunk.Session, frame_ids_to_remove: set[str]) -> int:
    """Remove all traces for the given frame IDs from the store.

    Rewrites all trace-indexed arrays in-place (keeping attributes) and updates
    the processed_frames index. Returns the number of traces removed.
    """
    store_obj = session.store
    root = zarr.open_group(store_obj, mode="a")

    if "frame_id" not in root:
        return 0

    frame_ids_arr = root["frame_id"][:]
    keep_mask = np.array([fid not in frame_ids_to_remove for fid in frame_ids_arr])
    n_removed = int((~keep_mask).sum())

    if n_removed > 0:
        n_total = len(frame_ids_arr)
        for key in list(root.keys()):
            arr = root[key]
            if not isinstance(arr, zarr.Array) or not arr.shape or arr.shape[0] != n_total:
                continue
            attrs = dict(arr.attrs)
            filtered = arr[:][keep_mask]
            root.create_array(key, data=filtered, chunks=arr.chunks, overwrite=True)
            root[key].attrs.update(attrs)

    if "processed_frames" in root:
        existing = root["processed_frames"][:]
        remaining = np.array([f for f in existing if f not in frame_ids_to_remove], dtype="U100")
        root.create_array("processed_frames", data=remaining, chunks=(1000,), overwrite=True)

    return n_removed


def clear_store(session: icechunk.Session) -> None:
    """Clear all data and frame tracking for a full reprocess."""
    store = session.store
    root = zarr.open_group(store, mode="a")
    for key in list(root.keys()):
        del root[key]


def update_frame_index(
    session: icechunk.Session,
    frame_collections: dict[str, str] | None = None,
) -> None:
    """Rebuild frame_index (uint16 per trace) and frame_names root attribute from frame_id.

    If ``frame_collections`` is given (mapping frame_id -> collection name), it
    is unioned into the existing ``frame_collections`` root attribute (parallel
    to ``frame_names``). The viewer uses this to show full collection names —
    parsing the year from the frame id alone is ambiguous when multiple
    collections share a year.
    """
    store = session.store
    root = zarr.open_group(store, mode="a")

    if "frame_id" not in root:
        return

    # Snapshot previous frame -> collection mapping before we overwrite frame_names.
    prev_names = list(root.attrs.get("frame_names", []) or [])
    prev_cols = list(root.attrs.get("frame_collections", []) or [])
    prev_mapping: dict[str, str] = dict(zip(prev_names, prev_cols))

    frame_ids = root["frame_id"][:]

    seen: dict[str, int] = {}
    frame_names: list[str] = []
    for fid in frame_ids:
        if fid not in seen:
            seen[fid] = len(frame_names)
            frame_names.append(fid)

    assert len(frame_names) <= 65535, "Too many frames for uint16"

    frame_index = np.array([seen[fid] for fid in frame_ids], dtype="uint16")
    root.create_array("frame_index", data=frame_index, chunks=(10000,), overwrite=True)
    root.attrs["frame_names"] = frame_names

    # Maintain frame_collections parallel to frame_names. Merge whatever new
    # entries we were given with the previous mapping so partial inputs don't
    # drop collection info for older frames.
    if frame_collections or prev_mapping:
        if frame_collections:
            prev_mapping.update(frame_collections)
        root.attrs["frame_collections"] = [prev_mapping.get(name, "") for name in frame_names]

    logger.info("Updated frame_index: %d traces, %d unique frames", len(frame_ids), len(frame_names))


def commit_session(session: icechunk.Session, message: str) -> str:
    """Commit the session and return the snapshot ID."""
    snapshot_id = session.commit(message)
    logger.info("Committed: %s (snapshot: %s)", message, snapshot_id)
    return snapshot_id
