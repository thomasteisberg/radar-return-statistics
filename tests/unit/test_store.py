import numpy as np
import pandas as pd
import pytest
import xarray as xr
import zarr

from radar_return_statistics import store as store_mod

EXPECTED_VARS = {
    "surface_twtt", "bed_twtt", "surface_elevation", "bed_elevation",
    "surface_power_dB", "bed_power_dB", "required_surface_snr_dB",
    "qc_pass", "frame_id",
}


def _make_result_ds(n_traces=5, frame_id="FRAME_001", hour_offset=0):
    slow_times = pd.date_range(f"2012-01-01 {hour_offset:02d}:00:00", periods=n_traces, freq="10s")
    return xr.Dataset(
        {
            "surface_twtt": ("slow_time", np.full(n_traces, 5e-6)),
            "bed_twtt": ("slow_time", np.full(n_traces, 15e-6)),
            "surface_elevation": ("slow_time", np.full(n_traces, 1800.0)),
            "bed_elevation": ("slow_time", np.full(n_traces, 500.0)),
            "surface_power_dB": ("slow_time", np.full(n_traces, 10.0)),
            "bed_power_dB": ("slow_time", np.full(n_traces, 6.0)),
            "required_surface_snr_dB": ("slow_time", np.full(n_traces, 3.0)),
            "qc_pass": ("slow_time", np.ones(n_traces, dtype=bool)),
            "frame_id": ("slow_time", [frame_id] * n_traces),
        },
        coords={
            "slow_time": slow_times,
            "latitude": ("slow_time", np.full(n_traces, -75.0)),
            "longitude": ("slow_time", np.full(n_traces, 160.0)),
        },
    )


@pytest.fixture
def local_repo(tmp_path):
    config = {"backend": "local", "path": str(tmp_path / "store")}
    return store_mod.open_or_create_repo(config)


def _read_root(repo):
    session = repo.readonly_session(branch="main")
    return zarr.open_group(session.store, mode="r")


def test_open_creates_repo(tmp_path):
    config = {"backend": "local", "path": str(tmp_path / "store")}
    repo1 = store_mod.open_or_create_repo(config)
    repo2 = store_mod.open_or_create_repo(config)
    assert repo1 is not None and repo2 is not None


def test_first_write_creates_all_variables(local_repo):
    session = local_repo.writable_session("main")
    store_mod.write_frame_results(session, "F1", _make_result_ds())
    store_mod.commit_session(session, "test")

    root = _read_root(local_repo)
    assert EXPECTED_VARS.issubset(set(root.keys()))
    assert root["surface_twtt"].shape[0] == 5


def test_append_increases_trace_count(local_repo):
    session = local_repo.writable_session("main")
    store_mod.write_frame_results(session, "F1", _make_result_ds(n_traces=5, hour_offset=0))
    store_mod.write_frame_results(session, "F2", _make_result_ds(n_traces=3, hour_offset=1))
    store_mod.commit_session(session, "test")

    root = _read_root(local_repo)
    assert root["surface_twtt"].shape[0] == 8


def test_first_write_uses_explicit_chunk_size(local_repo):
    session = local_repo.writable_session("main")
    store_mod.write_frame_results(session, "F1", _make_result_ds(n_traces=5))
    store_mod.commit_session(session, "test")

    root = _read_root(local_repo)
    expected = (store_mod.PER_TRACE_CHUNK_SIZE,)
    for name in ("surface_twtt", "latitude", "longitude", "slow_time"):
        assert root[name].chunks == expected, f"{name} chunks={root[name].chunks}"


def test_get_processed_frames(local_repo):
    session = local_repo.writable_session("main")
    store_mod.write_frame_results(session, "F1", _make_result_ds(hour_offset=0))
    store_mod.write_frame_results(session, "F2", _make_result_ds(hour_offset=1))
    store_mod.commit_session(session, "test")

    assert store_mod.get_processed_frames(local_repo) == {"F1", "F2"}


def test_clear_store(local_repo):
    session = local_repo.writable_session("main")
    store_mod.write_frame_results(session, "F1", _make_result_ds())
    store_mod.clear_store(session)
    store_mod.commit_session(session, "cleared")

    assert store_mod.get_processed_frames(local_repo) == set()
    root = _read_root(local_repo)
    assert "surface_twtt" not in root


def test_update_frame_index_consistency(local_repo):
    session = local_repo.writable_session("main")
    store_mod.write_frame_results(session, "F1", _make_result_ds(n_traces=4, frame_id="FRAME_A", hour_offset=0))
    store_mod.write_frame_results(session, "F2", _make_result_ds(n_traces=3, frame_id="FRAME_B", hour_offset=1))
    store_mod.update_frame_index(session)
    store_mod.commit_session(session, "test")

    root = _read_root(local_repo)

    frame_names = root.attrs["frame_names"]
    frame_index = root["frame_index"][:]
    frame_ids = root["frame_id"][:]

    assert root["frame_index"].dtype == np.dtype("uint16")
    assert isinstance(frame_names, list)
    assert set(frame_names) == {"FRAME_A", "FRAME_B"}
    assert len(frame_index) == 7

    reconstructed = [frame_names[i] for i in frame_index]
    np.testing.assert_array_equal(frame_ids, reconstructed)


def test_update_frame_index_writes_collections(local_repo):
    session = local_repo.writable_session("main")
    store_mod.write_frame_results(session, "F1", _make_result_ds(n_traces=3, frame_id="FRAME_A", hour_offset=0))
    store_mod.write_frame_results(session, "F2", _make_result_ds(n_traces=4, frame_id="FRAME_B", hour_offset=1))
    store_mod.update_frame_index(
        session,
        frame_collections={"FRAME_A": "season_a", "FRAME_B": "season_b"},
    )
    store_mod.commit_session(session, "test")

    root = _read_root(local_repo)
    names = list(root.attrs["frame_names"])
    cols = list(root.attrs["frame_collections"])
    assert len(names) == len(cols)
    assert dict(zip(names, cols)) == {"FRAME_A": "season_a", "FRAME_B": "season_b"}


def test_update_frame_index_preserves_prior_collections(local_repo):
    """A subsequent run with only new-frame collections must not blank out prior frames."""
    s1 = local_repo.writable_session("main")
    store_mod.write_frame_results(s1, "F1", _make_result_ds(n_traces=3, frame_id="FRAME_A", hour_offset=0))
    store_mod.update_frame_index(s1, frame_collections={"FRAME_A": "season_a"})
    store_mod.commit_session(s1, "first")

    s2 = local_repo.writable_session("main")
    store_mod.write_frame_results(s2, "F2", _make_result_ds(n_traces=4, frame_id="FRAME_B", hour_offset=1))
    store_mod.update_frame_index(s2, frame_collections={"FRAME_B": "season_b"})
    store_mod.commit_session(s2, "second")

    root = _read_root(local_repo)
    names = list(root.attrs["frame_names"])
    cols = list(root.attrs["frame_collections"])
    assert dict(zip(names, cols)) == {"FRAME_A": "season_a", "FRAME_B": "season_b"}


def test_schema_contract(local_repo):
    """All downstream-visible arrays have the expected names and coordinate lengths."""
    session = local_repo.writable_session("main")
    store_mod.write_frame_results(session, "F1", _make_result_ds())
    store_mod.update_frame_index(session)
    store_mod.commit_session(session, "test")

    root = _read_root(local_repo)

    assert EXPECTED_VARS.issubset(set(root.keys()))

    n = root["surface_twtt"].shape[0]
    assert root["latitude"].shape[0] == n
    assert root["longitude"].shape[0] == n
    assert root["frame_index"].shape[0] == n

    frame_names = root.attrs["frame_names"]
    assert all(isinstance(name, str) for name in frame_names)
    assert all(0 <= i < len(frame_names) for i in root["frame_index"][:])
