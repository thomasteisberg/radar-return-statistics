import pytest

from radar_return_statistics.config import load_config


def test_defaults_applied(tmp_path):
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text("store:\n  path: /tmp/test\n")
    cfg = load_config(cfg_file)

    assert cfg["processing"]["data_product"] == "CSARP_standard"
    assert cfg["processing"]["decimate_interval"] == "10s"
    assert cfg["processing"]["layer_margin_m"] == 50
    assert cfg["processing"]["ice_permittivity"] == 3.17
    assert cfg["store"]["backend"] == "local"
    assert cfg["qc"]["min_traces_after_qc"] == 10


def test_explicit_values_not_overridden(tmp_path):
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text("processing:\n  layer_margin_m: 100\n")
    cfg = load_config(cfg_file)
    assert cfg["processing"]["layer_margin_m"] == 100


def test_missing_file_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_config(tmp_path / "nonexistent.yaml")


def test_unknown_keys_pass_through(tmp_path):
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text("custom_key: custom_value\n")
    cfg = load_config(cfg_file)
    assert cfg["custom_key"] == "custom_value"
