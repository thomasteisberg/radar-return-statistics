# ASE Season Processing - 2026-04-12

Processing all available seasons in the G-H (Amundsen Sea Embayment) subregion.

Config: `config/config.yaml` with `subregion: "G-H"`, `decimate_interval: "10s"`, `min_traces_after_qc: 15`.

Store: S3 `s3://opr-radar-metrics/icechunk/ase`

## Summary

| # | Collection | Frames | Processed | Traces | Status | Issue |
|---|-----------|--------|-----------|--------|--------|-------|
| 1 | 2002_Antarctica_P3chile | 33 | 0 | 0 | **skipped** | No layer picks in OPR |
| 2 | 2004_Antarctica_P3chile | 21 | 0 | 0 | **skipped** | No layer picks in OPR |
| 3 | 2009_Antarctica_DC8 | 272 | 0 | 0 | **failed** | Missing `Heading` variable |
| 4 | 2009_Antarctica_TO | 428 | 0 | 0 | **failed** | Missing `Heading` + missing layers |
| 5 | 2010_Antarctica_DC8 | 114 | 0 | 0 | **failed** | Missing `Heading` variable |
| 6 | 2011_Antarctica_DC8 | 304 | 0 | 0 | **failed** | Missing `Heading` variable |
| 7 | 2012_Antarctica_DC8 | 222 | 186 | 6,120 | **done** | 36 frames failed QC |
| 8 | 2013_Antarctica_P3 | 2 | 2 | 82 | **done** | Spatial outlier (see note) |
| 9 | 2014_Antarctica_DC8 | 229 | 208 | 7,590 | **done** | 21 frames failed QC |
| 10 | 2016_Antarctica_DC8 | 202 | 176 | 6,373 | **done** | 26 frames failed QC |
| 11 | 2018_Antarctica_DC8 | 221 | 199 | 7,078 | **done** | 22 frames failed QC |

**Totals: 771 frames processed, 27,243 traces, 22,143 passing QC**

## Final Store State

```
Commit History:
  YVQF46PMC81D7TZX6280 | 2013_Antarctica_P3: Processed 2 frames (82 traces)
  CZDYX79XQTWMWY85PS6G | 2012_Antarctica_DC8: Processed 186 frames (6120 traces)
  Q4M3W368DQXWH5KFWZM0 | 2014_Antarctica_DC8: Processed 208 frames (7590 traces)
  GANVTGZQY2J5RWGE0FBG | 2016_Antarctica_DC8: Processed 176 frames (6373 traces)
  3M6T42WE7DFHSNM3EMH0 | 2018_Antarctica_DC8: Processed 150 frames (5326 traces)
  B4P3V6BNSNR87FKY0KR0 | Processed 49 frames (1752 traces)
  1CECHNKREP0F1RSTCMT0 | Repository initialized
```

## Issues

### Missing `Heading` variable (2009-2011 seasons)
The xopr QC `heading_change` check requires a `Heading` variable in the frame dataset.
Seasons 2009_DC8, 2009_TO, 2010_DC8, and 2011_DC8 all lack this variable, causing all
frames to raise `ValueError: Dataset is missing required variable 'Heading'`.

To process these seasons, the heading check would need to be disabled
(`max_heading_change_deg_per_km: null`) or xopr updated to gracefully skip the check
when Heading is unavailable.

### No layer picks (2002, 2004 P3chile)
All frames in both P3chile collections return "No layer data found" or "Failed to fetch
layer points." These early seasons predate reliable OPR layer coverage for this region.

### 2013_P3 spatial outlier
The 2 frames from `2013_Antarctica_P3` (`Data_20131126_01_067`, `Data_20131126_01_011`)
have longitude spanning [-180, 180] — they appear to be near the 180° meridian (Ross Ice
Shelf / Siple Coast area), not in the ASE. They were likely included due to the geometry
query intersecting their bounding box. These 82 traces pull the map visualization extent
to the entire continent.

### Duplicate commit (resolved)
A duplicate 2014 processing run committed before it could be stopped, adding 7,590
duplicate traces. Resolved by resetting the `main` branch to the pre-duplicate snapshot
`Q4M3W368DQXWH5KFWZM0` using `repo.reset_branch()`.

### QC threshold
Original `min_traces_after_qc: 50` was too high for 10s-decimated CReSIS frames (typically
20-40 traces per frame). Lowered to 15 to allow processing.
