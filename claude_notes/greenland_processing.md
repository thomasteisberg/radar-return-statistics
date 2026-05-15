# Greenland processing notes

Incremental backward addition of Greenland P3 seasons into
`s3://opr-radar-metrics/icechunk/greenland/`.

## Stored seasons (frame counts from frame_collections attr)

| Collection         | Frames |
|--------------------|-------:|
| 2017_Greenland_P3  | 1140 |
| 2014_Greenland_P3  | 1060 |
| 2019_Greenland_P3  |  874 |
| 2013_Greenland_P3  |  623 |
| 2018_Greenland_P3  |  604 |
| 2016_Greenland_P3  |  176 |
| **Total**          | **4477** (~138k QC-passing traces) |

## Cross-season crossover analyses

`scripts/analysis/season_crossovers.py config/config_greenland.yaml --collection <C>`
runs full find_crossovers (EPSG:3413 for Greenland), filters to pairs where
exactly one side is `<C>`, saves to `outputs/crossovers_greenland/<C>/`.

- **2014 vs 2016-2019**: N=2330. surface_elev RMS 19.7 m, bed_elev RMS 61.4 m,
  ice_thk RMS 59.7 m, RSSNR RMS 9.7 dB (mean +1.9).
- **2013 vs 2014/2016-2019**: N=1844. surface_elev RMS 22.5 m, bed_elev RMS
  59.0 m, RSSNR RMS 18.7 dB (mean +16.5) — notable calibration-like offset
  between 2013 and later seasons; surface_power flips sign vs 2014.

## The pre-2013 wall: missing `Heading`

2012_Greenland_P3 produced **0 usable frames**: 1791 frames raised
`ValueError: Dataset is missing required variable 'Heading'` from xopr's
turn-rejection QC check; the rest were QC-filtered for other reasons. The
pipeline correctly committed nothing.

Per the earlier season survey, 2009-2011 (DC8/TO) have the same gap. So with
the current QC config, the backward march realistically ends at **2013**.

Options if we want pre-2013:
1. Stop at 2013 (current state).
2. Synthesize `Heading` from consecutive GPS positions (bearing) in
   `processing.py` before QC runs. Principled; keeps QC consistent. Must be
   validated against a season that *has* Heading to confirm synthesized
   values agree.
3. Disable `max_heading_change_deg_per_km` for old seasons — rejected:
   breaks QC methodology consistency, contaminating crossover comparisons.

`config_greenland.yaml` has 2012 removed with a comment so it isn't retried
every run.

## Pipeline operational notes

- `processing.checkpoint_every` (default 1000): pipeline commits a
  `[checkpoint]` snapshot every N stored frames, plus a final `[run]` commit.
  Viewer hides `[checkpoint]` entries unless "Show checkpoints" is ticked.
- `max_workers: 4` for Greenland — 8 caused silent worker death + parent
  hang on the large (200 MB+) P3 frames. The `radar_cache/` dir makes
  re-runs fast (no re-download), so a killed run is cheap to resume.
- A run that stores nothing returns early without committing (store
  unchanged), so failed seasons are harmless besides wasted time.
