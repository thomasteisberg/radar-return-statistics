# Radar Return Statistics

Extract per-frame radar return statistics from the [xOPR](https://github.com/englacial/xopr)
archive and store results in a versioned [icechunk](https://icechunk.io/) store.

For each trace, the pipeline extracts surface and bed return power, two-way travel times,
elevations, and the required surface SNR metric. Results are decimated, QC-filtered, and
committed to icechunk with full version history.

## Documentation

- **[Processing data](docs/processing.md)** -- running the pipeline, managing collections,
  configuration reference, and generating visualizations
- **[Accessing the data](docs/data_access.md)** -- reading the icechunk store from Python
  and JavaScript, variable descriptions, and working with version history

## Quick start

```bash
uv sync
uv run python -m radar_return_statistics config/config.yaml
```

## Output variables

| Variable | Description |
|----------|-------------|
| `surface_power_dB` | Surface peak return power (dB) |
| `bed_power_dB` | Bed peak return power (dB) |
| `surface_elevation` | Surface elevation (m WGS84) |
| `bed_elevation` | Bed elevation (m WGS84) |
| `required_surface_snr_dB` | Geometric-spreading-corrected surface-to-bed power ratio (dB) |
| `surface_twtt`, `bed_twtt` | Two-way travel times (s) |
| `qc_pass` | Per-trace QC flag |
