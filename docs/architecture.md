This project processes radar sounder data retrieved through the xOPR library and
extracts per-frame return statistics, storing results in an icechunk versioned store.

xOPR: https://github.com/englacial/xopr

## Output metrics

Per-trace (resampled) values stored with `slow_time` dimension:
* `surface_twtt` - surface two-way travel time (peak within margin)
* `bed_twtt` - bed two-way travel time (peak within margin)
* `surface_elevation` - surface WGS84 elevation from layer picks
* `bed_elevation` - bed WGS84 elevation from layer picks
* `surface_power_dB` - surface peak power in dB
* `bed_power_dB` - bed peak power in dB
* `required_surface_snr_dB` - surface-to-bed power ratio corrected for geometric spreading (dB);
  matches RSSNR definition from https://github.com/thomasteisberg/required_surface_snr
* `frame_id` - source frame identifier

Coordinates: `latitude`, `longitude`, `elevation`

## Architecture

A **plain Python runner** (no snakemake) flat-maps over independent frames. Icechunk
handles versioning and incremental tracking (processed frame IDs stored in the zarr group).

### Modules

* `config.py` - loads YAML config
* `processing.py` - per-frame metric extraction (ports `extract_layer_peak_power` algorithm)
* `store.py` - icechunk read/write, frame tracking, commits
* `runner.py` - orchestration: query, diff, process (parallel), write, commit
* `__main__.py` - CLI entry point via click

### How to run

```bash
uv run python -m radar_return_statistics config/config.yaml
uv run python -m radar_return_statistics config/config.yaml --reprocess  # ignore existing frames
uv run python -m radar_return_statistics config/config.yaml -v           # debug logging
```

### Storage

Supports both local filesystem and S3 backends, configured via `store.backend` in the
YAML config. S3 uses `icechunk.s3_storage` with `from_env=True` for credential chain.
Set `AWS_PROFILE` for local development.

### Processing pipeline

1. Load config, open/create icechunk repo
2. Query frames from OPR matching region geometry
3. Filter to unprocessed frames (or all if `--reprocess`)
4. Process frames in parallel (`ProcessPoolExecutor`)
5. Write results sequentially to icechunk (append along `slow_time`)
6. Commit with summary message

# Features to implement later:

### Testing

The persistent icechunk store should allow for automated test to be setup where individual
frames are run and the output is compared against the current stored output. The test should
pass if the outputs are the same.

### Access and visualization

A small set of utilities should be provided for easily accessing and opening the output
stored in icechunk. These utility functions should include basic visualization, including
building a map of each of the metrics.

### GitHub pages visualization

An HTML representaiton of the icechunk dataset should be created that uses icechunk-js
(https://github.com/englacial/icechunk-js/) to stream the outputs for each metric
onto a map in the user's browser.