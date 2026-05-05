import numpy as np
import pandas as pd
import pytest
import xarray as xr

N_SLOW = 10
N_TWTT = 100
SURF_IDX = 20
BED_IDX = 70
# Data values chosen so peak powers are round numbers in dB:
#   10 * log10(SURF_VAL) = 10 dB, 10 * log10(BED_VAL) = 6 dB
SURF_VAL = 10.0
BED_VAL = 10**0.6


@pytest.fixture
def synthetic_frame():
    slow_times = pd.date_range("2012-01-01", periods=N_SLOW, freq="10s")
    twtt_vals = np.linspace(1e-6, 20e-6, N_TWTT)

    rng = np.random.default_rng(42)
    data = rng.uniform(0.001, 0.01, size=(N_SLOW, N_TWTT))
    data[:, SURF_IDX] = SURF_VAL
    data[:, BED_IDX] = BED_VAL

    return xr.Dataset(
        {"Data": (["slow_time", "twtt"], data)},
        coords={
            "slow_time": slow_times,
            "twtt": twtt_vals,
            "Elevation": ("slow_time", np.full(N_SLOW, 2000.0)),
            "Latitude": ("slow_time", np.linspace(-75.0, -74.9, N_SLOW)),
            "Longitude": ("slow_time", np.linspace(160.0, 160.1, N_SLOW)),
        },
    )


@pytest.fixture
def synthetic_layers(synthetic_frame):
    twtt_vals = synthetic_frame.twtt.values
    slow_times = synthetic_frame.slow_time.values

    def layer(twtt_val):
        return {"twtt": xr.DataArray(
            np.full(N_SLOW, twtt_val),
            dims=["slow_time"],
            coords={"slow_time": slow_times},
        )}

    return {
        "standard:surface": layer(twtt_vals[SURF_IDX]),
        "standard:bottom": layer(twtt_vals[BED_IDX]),
    }


@pytest.fixture
def minimal_proc_config():
    return {
        "processing": {
            "data_product": "CSARP_standard",
            "decimate_interval": None,
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
    }
