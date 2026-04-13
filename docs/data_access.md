# Accessing the Data

The processed radar return statistics are stored in an [icechunk](https://icechunk.io/)
versioned store backed by S3. The data is stored as zarr v3 arrays and can be accessed
from Python, JavaScript, or any zarr-compatible tool.

## Store location

| Store | S3 URL | Region |
|-------|--------|--------|
| ASE (Amundsen Sea Embayment) | `s3://opr-radar-metrics/icechunk/ase` | G-H subregion |

HTTP access: `https://opr-radar-metrics.s3.us-west-2.amazonaws.com/icechunk/ase/`

## Variables

All variables share a single `slow_time` dimension (one entry per decimated trace).

| Variable | Description | Units |
|----------|-------------|-------|
| `latitude` | Trace latitude | degrees |
| `longitude` | Trace longitude | degrees |
| `elevation` | Aircraft WGS84 elevation | m |
| `surface_twtt` | Surface two-way travel time (peak within margin) | s |
| `bed_twtt` | Bed two-way travel time (peak within margin) | s |
| `surface_elevation` | Surface WGS84 elevation from layer picks | m |
| `bed_elevation` | Bed WGS84 elevation from layer picks | m |
| `surface_power_dB` | Surface peak return power | dB |
| `bed_power_dB` | Bed peak return power | dB |
| `required_surface_snr_dB` | Geometric-spreading-corrected surface-to-bed power ratio | dB |
| `qc_pass` | Whether the trace passed QC checks | bool (0/1) |
| `frame_id` | Source frame identifier | string |
| `slow_time` | CF-encoded timestamp | seconds since 1970-01-01 |

The `processed_frames` array tracks which frame IDs have been ingested.

## Python access

### Using icechunk + zarr directly

```python
import icechunk
import zarr
import numpy as np

storage = icechunk.s3_storage(
    bucket="opr-radar-metrics",
    prefix="icechunk/ase",
    region="us-west-2",
    from_env=True,  # uses AWS credential chain
)
repo = icechunk.Repository.open(storage=storage)
session = repo.readonly_session(branch="main")
root = zarr.open_group(session.store, mode="r")

# Read arrays
lat = root["latitude"][:]
lon = root["longitude"][:]
surface_power = root["surface_power_dB"][:]
qc = root["qc_pass"][:].astype(bool)

# Filter to QC-passing traces
lat_good = lat[qc]
surface_power_good = surface_power[qc]
```

### Loading as xarray Dataset

```python
import xarray as xr
from xarray.coding.times import decode_cf_datetime

# Decode slow_time from CF encoding
st_raw = root["slow_time"][:]
units = root["slow_time"].attrs.get("units", "seconds since 1970-01-01")
calendar = root["slow_time"].attrs.get("calendar", "proleptic_gregorian")
slow_time = decode_cf_datetime(st_raw, units, calendar)

ds = xr.Dataset(
    {
        "surface_power_dB": ("trace", root["surface_power_dB"][:]),
        "bed_power_dB": ("trace", root["bed_power_dB"][:]),
        "surface_elevation": ("trace", root["surface_elevation"][:]),
        "bed_elevation": ("trace", root["bed_elevation"][:]),
    },
    coords={
        "latitude": ("trace", root["latitude"][:]),
        "longitude": ("trace", root["longitude"][:]),
        "slow_time": ("trace", slow_time),
    },
)
```

Note: `xr.open_zarr()` cannot be used directly because the `processed_frames` array
lacks xarray dimension metadata. Use the zarr API to read arrays, then construct the
Dataset manually as shown above.

### Filtering by frame

```python
frame_ids = root["frame_id"][:]
mask = frame_ids == "Data_20181022_01_010"
lat_frame = root["latitude"][:][mask]
```

### Reading historical snapshots

Icechunk stores the full history. You can read data at any previous commit:

```python
for snap in repo.ancestry(branch="main"):
    print(f"{snap.id} | {snap.written_at} | {snap.message}")

# Open a specific snapshot
session = repo.readonly_session(snapshot_id="SNAPSHOT_ID_HERE")
root = zarr.open_group(session.store, mode="r")
```

## JavaScript access

The store can be read from the browser using `@carbonplan/icechunk-js` and `zarrita`:

```javascript
import { IcechunkStore } from "@carbonplan/icechunk-js";
import * as zarr from "zarrita";

const STORE_URL = "https://opr-radar-metrics.s3.us-west-2.amazonaws.com/icechunk/ase/";
const store = await IcechunkStore.open(STORE_URL, { branch: "main" });

const root = zarr.root(store);
const lat = await zarr.get(await zarr.open(root.resolve("/latitude"), { kind: "array" }));
const lon = await zarr.get(await zarr.open(root.resolve("/longitude"), { kind: "array" }));
```

The `web/` directory contains a full interactive map viewer built with this approach.

## Visualization tools

### Static maps

Generate maps for all variables:

```bash
uv run python -m radar_return_statistics.visualize_map config/config.yaml --output-dir outputs/maps
```

### Per-frame profiles

Generate elevation and return power profiles for a single frame:

```bash
uv run python -m radar_return_statistics.visualize_frame config/config.yaml Data_20181022_01_010
```

### Interactive web viewer

Run the browser-based map viewer locally:

```bash
cd web && npm install && npx vite
```

The viewer streams data directly from S3, supports variable switching, basemap selection,
and navigating the icechunk version history.
