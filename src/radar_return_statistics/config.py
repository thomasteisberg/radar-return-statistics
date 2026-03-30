from pathlib import Path

import yaml


def load_config(config_path: str | Path) -> dict:
    """Load and return config from YAML file."""
    config_path = Path(config_path)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path) as f:
        config = yaml.safe_load(f)

    # Apply defaults
    config.setdefault("opr", {})
    config.setdefault("region", {})
    config.setdefault("query", {})
    config.setdefault("processing", {})
    config.setdefault("store", {})

    config["processing"].setdefault("data_product", "CSARP_standard")
    config["processing"].setdefault("decimate_interval", "10s")
    config["processing"].setdefault("layer_margin_m", 50)
    config["processing"].setdefault("ice_permittivity", 3.17)
    config["processing"].setdefault("max_workers", 4)
    config.setdefault("qc", {})
    config["qc"].setdefault("max_heading_change_deg_per_km", None)
    config["qc"].setdefault("min_ice_thickness_m", None)
    config["qc"].setdefault("min_agl_m", None)
    config["qc"].setdefault("min_bed_snr_db", None)
    config["qc"].setdefault("min_traces_after_qc", 10)
    config["store"].setdefault("backend", "local")
    config["store"].setdefault("path", "outputs/icechunk_store")

    return config
