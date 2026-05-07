# Plan: MultisystemAGASEA Cross-Dataset Comparison (Radargram-based)

**Date:** 2026-05-07 (revised from 2026-05-06)  
**Status:** In progress — extractions running

## Background and motivation

Earlier work (see `claude_notes/csarp_standard_vs_qlook_rssnr.md`) showed that the AGASEA `reflectivity` variable is not comparable to OPR's `required_surface_snr_dB`. AGASEA reflectivity is an absolute (survey-mean-referenced) bed echo power; OPR RSSNR is a surface-to-bed received power ratio. The ~38 dB median offset is a definitional gap, not a calibration error.

The fix is to go back to the radargrams and compute RSSNR the same way OPR does: find the peak surface return power and peak bed return power in the radargram fast-time axis, then RSSNR = 10·log₁₀(P_surf / P_bed). Absolute calibration cancels in the ratio, so uncalibrated linear power units are fine.

---

## Datasets

### OPR ASE store (`config/config.yaml`)
- Instrument: CReSIS MCoRDS, ~195 MHz
- 771 frames, 27,243 total traces (22,143 QC-passing)
- S3 icechunk at `s3://opr-radar-metrics/icechunk/ase`
- Variables used: `latitude`, `longitude`, `surface_twtt`, `bed_twtt`, `required_surface_snr_dB`, `qc_pass`
- Survey period: 2012–2018 (five campaigns)

| Year | Frames | Traces |
|------|-------:|-------:|
| 2012 |    186 |  6,120 |
| 2013 |      2 |     82 |
| 2014 |    208 |  7,590 |
| 2016 |    176 |  6,373 |
| 2018 |    199 |  7,078 |

### AGASEA radargrams (`/media/thomasteisberg/Data/MultisystemAGASEA/`)
Two instruments, 2004–2005:

**HiCARS (UTIG) — primary:**
- 60 MHz, 15 MHz bandwidth; 10 coherent + 5 incoherent summations
- Two gain channels: `data_hi_gain`, `data_lo_gain`; hi gain is 39 dB more sensitive than lo gain
- Flight transects: DRP*, X*, Y* series; organized in archives now being extracted
- No radiometric reliability caveats
- ~4× more spatial overlap with OPR ASE than PASIN (226K vs 56K matched pairs from prior work)

**PASIN (BAS) — secondary:**
- 150 MHz, 15 MHz bandwidth; 70 coherent + 4 incoherent summations
- Single channel: `data` (uncalibrated linear power)
- **Caveat:** Upper ~300-500m of ice radiometrically unreliable (deep sounding pulse near-range artefacts). The surface return is in this zone. Treat PASIN RSSNR as lower-confidence.
- Flight transects: b01, b02, b04, b11–b17, b21, b23, b24, b32; already extracted

**Radargram file format (both instruments):**
Per-segment NetCDF, each covering ~50 km of flight line:
- `latitude`, `longitude` — WGS84, 1×M (one per along-track sample)
- `elevation` — aircraft height above WGS84 ellipsoid, meters, 1×M
- `fast-time` — two-way travel time in seconds, 1×N range samples
- `data` / `data_hi_gain` / `data_lo_gain` — received linear power, M×N matrix

**Processed Results (`/media/thomasteisberg/Data/MultisystemAGASEA/Results/`):**
One NetCDF per flight transect with: `latitude`, `longitude`, `radar_height` (AGL to ice surface, m), `ice_thickness` (m), `reflectivity`, `atten_rate`. The layer picks embedded in these files are used to locate surface and bed in the radargrams.

---

## Algorithm

### Step 1: Link Results traces to radargram traces

For each Results file (e.g., `X03a.nc`):
1. Find all radargram segments for that flight (e.g., `X03a/X03a_1.nc`, `X03a/X03a_2.nc`, …)
2. Concatenate all segments into one array of (lat, lon, elevation, fast_time, data)
3. Build a cKDTree from radargram lat/lon; for each Results trace query nearest radargram trace
4. Keep matches within a small threshold (e.g., 500 m) to handle any decimation differences

### Step 2: Locate surface and bed in fast-time axis

For each matched pair (Results trace ↔ radargram trace):

```
# Surface: aircraft AGL clearance from Results
t_surf = 2 * radar_height / c              # TWTT to ice surface (air propagation)
idx_surf = argmin(|fast_time - t_surf|)

# Bed: surface TWTT + two-way ice travel time
n_ice = 1.78  (from Results README: constant index of refraction)
t_bed = t_surf + 2 * ice_thickness / (c / n_ice)
idx_bed = argmin(|fast_time - t_bed|)
```

### Step 3: Extract peak power in window

```
margin = 10  # samples (~68 m)  — tune after inspecting data
P_surf = max(data[idx_surf - margin : idx_surf + margin])
P_bed  = max(data[idx_bed  - margin : idx_bed  + margin ])
RSSNR  = 10 * log10(P_surf / P_bed)
```

### Step 4: HiCARS gain channel handling

Inspect both channels at the surface and bed for a sample of traces:
- If hi_gain surface samples are visibly clipped (constant or decreasing toward peak), use lo_gain + 39 dB for the surface
- Otherwise use hi_gain for both
- Apply gain correction consistently so the ratio is always on the same effective scale

A simple rule: use `data_lo_gain` for surface (bright specular return, likely to saturate hi_gain) and `data_hi_gain` for bed (weak deep return, better SNR on hi gain). RSSNR then requires adding 39 dB:

```
RSSNR = 10 * log10(P_surf_lo / P_bed_hi) + 39
```

Verify this choice by checking crossovers — self-consistent crossovers indicate the 39 dB offset is applied correctly.

### Step 5: QC filter

Drop traces where:
- `radar_height` or `ice_thickness` is NaN in Results
- `t_surf` or `t_bed` falls outside the radargram fast-time range
- Nearest radargram trace distance > 500 m
- RSSNR is not finite
- For PASIN: optionally flag (but retain) all traces as low-confidence

---

## Implementation

### Script: `scripts/analysis/agasea_rssnr.py`

Processes AGASEA radargrams to produce a flat CSV of (lat, lon, instrument, flight, RSSNR, ice_thickness), one row per QC-passing Results trace that is successfully matched to a radargram.

```
uv run python scripts/analysis/agasea_rssnr.py \
    /media/thomasteisberg/Data/MultisystemAGASEA \
    --output outputs/agasea_rssnr.csv \
    [-v]
```

Output columns:
- `lat`, `lon` — from Results (or average of Results+radargram match)
- `flight` — Results flight name (e.g., `X03a`, `b01`)
- `instrument` — `HiCARS` or `PASIN`
- `ice_thickness` — from Results (m)
- `surface_power_dB`, `bed_power_dB` — in linear-dB of the uncalibrated power units
- `rssnr` — surface_power_dB − bed_power_dB (+ 39 if mixing gain channels)
- `match_dist_m` — distance between Results trace and matched radargram trace

### Script: `scripts/analysis/agasea_comparison.py`

Spatial comparison between the new radargram-derived RSSNR and OPR ASE. Mirrors `multisystem_vs_opr.py` but loads the CSV from `agasea_rssnr.py` instead of the Results NetCDF files.

```
uv run python scripts/analysis/agasea_comparison.py \
    outputs/agasea_rssnr.csv \
    config/config.yaml \
    --threshold 2000 \
    --output outputs/agasea_comparison \
    [-v]
```

Reuse `match_datasets`, `make_map`, `make_scatter`, `make_differences`, `_summary_stats` from `multisystem_vs_opr.py` (refactor shared code into `src/` if significant overlap).

Output files:
- `outputs/agasea_comparison/matched_pairs.csv`
- `outputs/agasea_comparison/summary.csv`
- `outputs/agasea_comparison/map.png`
- `outputs/agasea_comparison/scatter.png`
- `outputs/agasea_comparison/differences.png`

---

## Internal crossover check

Before the OPR comparison, validate the radargram RSSNR by running AGASEA internal crossovers (reuse `src/radar_return_statistics/crossovers.py`) on the output CSV. Same-instrument crossovers should agree to within a few dB; HiCARS–PASIN crossovers will show a systematic offset due to the 135 MHz frequency difference affecting Fresnel reflectivity and englacial scattering.

---

## Milestones

1. **Extractions complete** — all UTIG archives extracted; inspect one file to confirm `data_hi_gain` / `data_lo_gain` structure and value ranges
2. **agasea_rssnr.py** — runs on one flight, produces plausible RSSNR values (−20 to +80 dB range)
3. **Internal crossover check** — HiCARS self-crossovers show RMS < 5 dB on RSSNR; if > 5 dB investigate gain channel handling
4. **agasea_comparison.py** — produces matched pairs with N > 1000; examine scatter plot; expect tighter correlation than the prior Results-based comparison

---

## Key caveats

1. **Time gap:** AGASEA 2004–2005 vs OPR 2012–2018; ice thickness differences reflect real glacier change
2. **Frequency:** 60 MHz (HiCARS) vs 195 MHz (MCoRDS) — different Fresnel reflectivity, different englacial scattering. RSSNR differences between instruments are physics, not error
3. **Processing:** HiCARS uses unfocused SAR (10+5 looks); OPR uses CSARP_standard (full focused). This affects the coherent vs incoherent gain on the surface vs bed, but the CSARP_standard/qlook experiment showed the effect on RSSNR is small (3–4 dB), not 40 dB
4. **PASIN near-range:** Surface return may be affected by deep-pulse sidelobe artefacts; treat PASIN RSSNR with lower confidence

---

## Completed (prior work, now superseded)

- `scripts/analysis/multisystem_crossovers.py` — AGASEA internal crossovers using Results files; valid for ice_thickness
- `scripts/analysis/multisystem_vs_opr.py` — Results-based AGASEA vs OPR; valid for ice_thickness only; RSSNR comparison not meaningful
- `src/radar_return_statistics/crossovers.py` — shared crossover logic; still used
- CSARP_standard vs CSARP_qlook experiment — refuted coherent SAR as the cause of the 38 dB offset
