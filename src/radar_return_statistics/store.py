import logging

import numpy as np
import icechunk
import xarray as xr
import zarr
from xarray.coding.times import encode_cf_datetime

logger = logging.getLogger(__name__)


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
        results_ds.to_zarr(store, mode="w")
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


def clear_store(session: icechunk.Session) -> None:
    """Clear all data and frame tracking for a full reprocess."""
    store = session.store
    root = zarr.open_group(store, mode="a")
    for key in list(root.keys()):
        del root[key]


def commit_session(session: icechunk.Session, message: str) -> str:
    """Commit the session and return the snapshot ID."""
    snapshot_id = session.commit(message)
    logger.info("Committed: %s (snapshot: %s)", message, snapshot_id)
    return snapshot_id
