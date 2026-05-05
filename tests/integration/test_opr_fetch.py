import pytest
from xopr import OPRConnection

from radar_return_statistics.runner import _get_region_geometry

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def opr():
    return OPRConnection(cache_dir=None)


@pytest.fixture(scope="module")
def frames(opr):
    geometry = _get_region_geometry({
        "area": "antarctic",
        "name": ["David", "Drygalski"],
        "type": None,
        "regions": "East",
    })
    return opr.query_frames(geometry=geometry, max_items=3)


def test_query_returns_frames(frames):
    assert len(frames) > 0


def test_frame_ids_are_strings(frames):
    assert all(isinstance(fid, str) for fid in frames.index)


def test_load_frame_structure(opr, frames):
    if len(frames) == 0:
        pytest.skip("No frames returned by OPR")

    frame = opr.load_frame(frames.iloc[0], data_product="CSARP_standard")

    assert "slow_time" in frame.dims
    assert "Data" in frame
    assert "Latitude" in frame or "Latitude" in frame.coords
    assert "Longitude" in frame or "Longitude" in frame.coords
    assert "Elevation" in frame or "Elevation" in frame.coords
    assert len(frame.slow_time) > 0


def test_get_layers_returns_surface_and_bed(opr, frames):
    if len(frames) == 0:
        pytest.skip("No frames returned by OPR")

    frame = opr.load_frame(frames.iloc[0], data_product="CSARP_standard")
    layers = opr.get_layers(frame, include_geometry=False)

    assert layers is not None
    assert "standard:surface" in layers
    assert "standard:bottom" in layers
    assert "twtt" in layers["standard:surface"]
    assert "twtt" in layers["standard:bottom"]
