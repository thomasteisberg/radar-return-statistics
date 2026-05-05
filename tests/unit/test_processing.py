import types

import numpy as np
import pytest
import scipy.constants

from radar_return_statistics.processing import (
    _build_qc_checks,
    extract_layer_peak_power,
    process_frame,
)
from tests.conftest import BED_IDX, BED_VAL, SURF_IDX, SURF_VAL


def test_extract_peak_finds_correct_twtt(synthetic_frame, synthetic_layers):
    expected_twtt = synthetic_frame.twtt.values[SURF_IDX]
    peak_twtt, _ = extract_layer_peak_power(
        synthetic_frame,
        synthetic_layers["standard:surface"]["twtt"],
        margin_twtt=1e-6,
    )
    np.testing.assert_allclose(peak_twtt.values, expected_twtt, rtol=1e-9)


def test_extract_peak_power_correct_db(synthetic_frame, synthetic_layers):
    _, peak_power = extract_layer_peak_power(
        synthetic_frame,
        synthetic_layers["standard:surface"]["twtt"],
        margin_twtt=1e-6,
    )
    expected_dB = 10 * np.log10(SURF_VAL)
    np.testing.assert_allclose(peak_power.values, expected_dB, rtol=1e-9)


def test_build_qc_checks_excludes_none():
    checks = _build_qc_checks({
        "max_heading_change_deg_per_km": None,
        "min_ice_thickness_m": 100,
        "min_agl_m": None,
        "min_bed_snr_db": 5.0,
    })
    assert "heading_change" not in checks
    assert "minimum_agl" not in checks
    assert checks["ice_thickness_threshold"] == {"min_thickness_m": 100}
    assert checks["snr_bed_pick"] == {"min_snr_db": 5.0}


def test_build_qc_checks_all_none():
    assert _build_qc_checks({}) == {}


def test_process_frame_output_variables(mocker, synthetic_frame, synthetic_layers, minimal_proc_config):
    opr = mocker.MagicMock()
    opr.load_frame.return_value = synthetic_frame
    opr.get_layers.return_value = synthetic_layers

    ds = process_frame(opr, types.SimpleNamespace(name="FRAME_001"), minimal_proc_config)

    assert ds is not None
    assert set(ds.data_vars) == {
        "surface_twtt", "bed_twtt", "surface_elevation", "bed_elevation",
        "surface_power_dB", "bed_power_dB", "required_surface_snr_dB",
        "qc_pass", "frame_id",
    }


def test_process_frame_frame_id_filled(mocker, synthetic_frame, synthetic_layers, minimal_proc_config):
    opr = mocker.MagicMock()
    opr.load_frame.return_value = synthetic_frame
    opr.get_layers.return_value = synthetic_layers

    ds = process_frame(opr, types.SimpleNamespace(name="MY_FRAME"), minimal_proc_config)

    assert all(fid == "MY_FRAME" for fid in ds["frame_id"].values)


def test_process_frame_returns_none_on_missing_bed_layer(mocker, synthetic_frame, minimal_proc_config):
    opr = mocker.MagicMock()
    opr.load_frame.return_value = synthetic_frame
    opr.get_layers.return_value = {"standard:surface": {}}  # no bed

    ds = process_frame(opr, types.SimpleNamespace(name="FRAME"), minimal_proc_config)
    assert ds is None


def test_process_frame_returns_none_on_layer_exception(mocker, synthetic_frame, minimal_proc_config):
    opr = mocker.MagicMock()
    opr.load_frame.return_value = synthetic_frame
    opr.get_layers.side_effect = RuntimeError("layer load failed")

    ds = process_frame(opr, types.SimpleNamespace(name="FRAME"), minimal_proc_config)
    assert ds is None


def test_rssnr_matches_geometric_spreading_formula(mocker, synthetic_frame, synthetic_layers, minimal_proc_config):
    """RSSNR matches the reference formula: surf_power - geom_surf - (bed_power - geom_bed)."""
    opr = mocker.MagicMock()
    opr.load_frame.return_value = synthetic_frame
    opr.get_layers.return_value = synthetic_layers

    ds = process_frame(opr, types.SimpleNamespace(name="FRAME"), minimal_proc_config)

    twtt = synthetic_frame.twtt.values
    surf_twtt = twtt[SURF_IDX]
    bed_twtt = twtt[BED_IDX]
    ice_permittivity = minimal_proc_config["processing"]["ice_permittivity"]
    c = scipy.constants.c
    n = np.sqrt(ice_permittivity)
    v_ice = c / n

    r_surf = c * surf_twtt / 2
    ice_thickness = v_ice / 2 * (bed_twtt - surf_twtt)
    r_bed_eff = r_surf + ice_thickness / n

    expected = 10 * np.log10(SURF_VAL * r_surf**2 / (BED_VAL * r_bed_eff**2))
    np.testing.assert_allclose(ds["required_surface_snr_dB"].values, expected, rtol=1e-5)
