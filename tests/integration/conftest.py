import pytest


@pytest.fixture
def local_config(tmp_path):
    """Pipeline config using a local icechunk store, limited to 1 frame for speed."""
    return {
        "opr": {"cache_dir": None},
        "region": {
            "area": "antarctic",
            "name": ["David", "Drygalski"],
            "type": None,
            "regions": "East",
        },
        "query": {"collections": None, "max_items": 1},
        "processing": {
            "data_product": "CSARP_standard",
            "decimate_interval": "10s",
            "layer_margin_m": 50,
            "ice_permittivity": 3.17,
            "max_workers": 1,
        },
        "qc": {
            "max_heading_change_deg_per_km": None,
            "min_ice_thickness_m": None,
            "min_agl_m": None,
            "min_bed_snr_db": None,
            "min_traces_after_qc": 1,
        },
        "store": {"backend": "local", "path": str(tmp_path / "store")},
    }
