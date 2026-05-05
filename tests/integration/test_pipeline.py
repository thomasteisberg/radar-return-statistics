import copy

import numpy as np
import pytest
import zarr

from radar_return_statistics import store as store_mod
from radar_return_statistics.runner import run

pytestmark = pytest.mark.integration

EXPECTED_VARS = {
    "surface_twtt", "bed_twtt", "surface_elevation", "bed_elevation",
    "surface_power_dB", "bed_power_dB", "required_surface_snr_dB",
    "qc_pass", "frame_id",
}


def _open_root(repo):
    session = repo.readonly_session(branch="main")
    return zarr.open_group(session.store, mode="r")


def test_pipeline_writes_valid_store(local_config):
    run(config=local_config)

    repo = store_mod.open_or_create_repo(local_config["store"])
    root = _open_root(repo)

    assert EXPECTED_VARS.issubset(set(root.keys()))
    assert "frame_index" in root
    assert "frame_names" in root.attrs

    n = root["surface_twtt"].shape[0]
    assert n > 0
    assert root["latitude"].shape[0] == n
    assert root["longitude"].shape[0] == n
    assert root["frame_index"].shape[0] == n


def test_pipeline_frame_index_consistent_with_frame_id(local_config):
    run(config=local_config)

    repo = store_mod.open_or_create_repo(local_config["store"])
    root = _open_root(repo)

    frame_ids = root["frame_id"][:]
    frame_index = root["frame_index"][:]
    frame_names = root.attrs["frame_names"]

    reconstructed = [frame_names[i] for i in frame_index]
    np.testing.assert_array_equal(frame_ids, reconstructed)


def test_pipeline_idempotent(local_config):
    """Running twice with the same frames produces no additional traces."""
    run(config=local_config)
    repo = store_mod.open_or_create_repo(local_config["store"])
    n_first = _open_root(repo)["surface_twtt"].shape[0]

    run(config=local_config)
    n_second = _open_root(repo)["surface_twtt"].shape[0]

    assert n_first == n_second


def test_reprocess_replaces_data(local_config, tmp_path):
    """--reprocess clears and rewrites the store, ending at the same trace count."""
    run(config=local_config)
    repo = store_mod.open_or_create_repo(local_config["store"])
    n_first = _open_root(repo)["surface_twtt"].shape[0]

    run(config=local_config, reprocess=True)
    n_reprocessed = _open_root(repo)["surface_twtt"].shape[0]

    assert n_reprocessed == n_first
