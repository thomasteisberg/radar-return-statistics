"""Regression: re-process a known frame and compare against the stored output.

Opens the public crosssystem S3 store (read-only, no write credentials needed),
reads the stored metric values for a known frame, then re-processes that frame
via OPR and asserts the outputs match. Any discrepancy means the algorithm has
changed since the store was last built.
"""
import numpy as np
import pytest
import zarr
from xopr import OPRConnection

from radar_return_statistics import store as store_mod
from radar_return_statistics.processing import process_frame
from radar_return_statistics.runner import _get_region_geometry

pytestmark = pytest.mark.integration

_S3_CONFIG = {
    "backend": "s3",
    "s3_bucket": "opr-radar-metrics",
    "s3_prefix": "icechunk/crosssystem",
    "s3_region": "us-west-2",
}

# Must match the processing config used to build the crosssystem store.
_PROC_CONFIG = {
    "processing": {
        "data_product": "CSARP_standard",
        "decimate_interval": "10s",
        "layer_margin_m": 50,
        "ice_permittivity": 3.17,
        "max_workers": 1,
    },
    "qc": {
        "max_heading_change_deg_per_km": 2.0,
        "min_ice_thickness_m": 100,
        "min_agl_m": 50,
        "min_bed_snr_db": None,
        "min_traces_after_qc": 15,
    },
}

_REGION = {
    "area": "antarctic",
    "name": ["David", "Drygalski", "Moscow", "Moscow_University"],
}

# Confirmed present in the crosssystem store.
KNOWN_FRAME_ID = "Data_20131127_01_021"

METRIC_VARS = [
    "surface_twtt",
    "bed_twtt",
    "surface_power_dB",
    "bed_power_dB",
    "required_surface_snr_dB",
]


@pytest.fixture(scope="module")
def crosssystem_root():
    import icechunk

    storage = icechunk.s3_storage(
        bucket=_S3_CONFIG["s3_bucket"],
        prefix=_S3_CONFIG["s3_prefix"],
        region=_S3_CONFIG["s3_region"],
        anonymous=True,
    )
    repo = icechunk.Repository.open(storage=storage)
    session = repo.readonly_session(branch="main")
    return zarr.open_group(session.store, mode="r")


@pytest.fixture(scope="module")
def opr_stac_item():
    """Fetch the STAC item for KNOWN_FRAME_ID, or skip if unavailable."""
    opr = OPRConnection(cache_dir=None)
    geometry = _get_region_geometry(_REGION)
    frames = opr.query_frames(geometry=geometry)
    if KNOWN_FRAME_ID not in frames.index:
        pytest.skip(f"{KNOWN_FRAME_ID} not found in OPR query results")
    return opr, frames.loc[KNOWN_FRAME_ID]


def test_reprocess_matches_stored_output(crosssystem_root, opr_stac_item):
    """Re-processing a known frame exactly reproduces the stored metric values."""
    opr, stac_item = opr_stac_item

    frame_names = crosssystem_root.attrs.get("frame_names", [])
    if KNOWN_FRAME_ID not in frame_names:
        pytest.skip(f"{KNOWN_FRAME_ID} not in store frame_names")

    frame_idx = frame_names.index(KNOWN_FRAME_ID)
    frame_index = crosssystem_root["frame_index"][:]
    mask = frame_index == frame_idx

    stored = {var: crosssystem_root[var][:][mask] for var in METRIC_VARS}

    ds = process_frame(opr, stac_item, _PROC_CONFIG)
    if ds is None:
        pytest.skip(f"{KNOWN_FRAME_ID} could not be re-processed")

    assert len(ds.slow_time) == mask.sum(), (
        f"Trace count mismatch: stored {mask.sum()}, re-processed {len(ds.slow_time)}"
    )

    for var in METRIC_VARS:
        np.testing.assert_allclose(
            ds[var].values,
            stored[var],
            rtol=1e-6,
            equal_nan=True,
            err_msg=f"{var} differs from stored values — possible algorithm change",
        )
