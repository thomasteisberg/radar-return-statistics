# CSARP_standard vs CSARP_qlook RSSNR experiment

**Date:** 2026-05-07  
**Context:** Testing whether coherent SAR processing gain explains the ~37 dB RSSNR offset between the OPR ASE dataset (CSARP_standard) and the AGASEA dataset (incoherent stacking).

---

## Motivation

The OPR RSSNR has a median of ~60 dB. The AGASEA `rssnr_equiv = -(reflectivity - 2·atten_rate·ice_thickness/1000)` has a median of ~18 dB — a ~40 dB gap. The hypothesis was that this gap arises because OPR uses CSARP (coherent synthetic aperture processing), which gives disproportionately more gain to the specular ice surface return than the diffuse bed return.

---

## Prediction

**Coherent gain model:** When coherently integrating N pulses, a specular target gains N² in power (amplitude adds constructively), while a diffuse/rough target gains only N (incoherent average). The RSSNR benefit of coherent processing over incoherent is therefore:

```
RSSNR_coherent - RSSNR_incoherent = 10·log₁₀(N)
```

For two different aperture lengths N₁ (standard) > N₂ (qlook):

```
RSSNR_standard - RSSNR_qlook = 10·log₁₀(N₁/N₂)
```

CSARP_qlook has ~10× fewer output slow-time traces than CSARP_standard for the same frame (e.g., 1700 vs 176 traces observed for Data_20121016_03_001), suggesting the coherent integration aperture is shorter by approximately a factor of 10.

**Predicted difference:** RSSNR(standard) − RSSNR(qlook) ≈ 10·log₁₀(10) = **+10 dB**  
(standard should be higher; range 7–15 dB depending on exact aperture ratio)

If this 10 dB matched the ~40 dB AGASEA gap, it would require N_qlook ≈ 1 (essentially single-pulse, no coherent integration). If qlook uses some coherent processing, the standard-vs-qlook gap would be smaller than the AGASEA gap.

---

## Experiment

**Season:** 2012_Antarctica_DC8  
**Frames:** 20 frames from the G-H subregion of the ASE  
**Processing config:** 10 s decimation, 50 m layer margin, QC applied  
**Code:** `radar_return_statistics.processing.process_frame()` with `data_product` switched between `CSARP_standard` and `CSARP_qlook`

### Results

| Product | N traces | surface_power_dB (median) | bed_power_dB (median) | RSSNR (median) | RSSNR std |
|---|---:|---:|---:|---:|---:|
| CSARP_standard | 480 | −46.0 dB | −81.2 dB | 31.5 dB | 19.0 dB |
| CSARP_qlook    | 421 | −63.6 dB | −101.0 dB | 35.3 dB | 19.0 dB |
| **Difference (std − qlook)** | | **+17.6 dB** | **+19.8 dB** | **−3.8 dB** | |

Single-frame spot check (Data_20121016_03_001):

| Product | surface_power_dB | bed_power_dB | RSSNR |
|---|---:|---:|---:|
| CSARP_standard | −50.7 dB | −109.7 dB | 58.5 dB |
| CSARP_qlook    | −75.1 dB | −131.4 dB | 55.7 dB |
| Difference     | +24.4 dB | +21.7 dB | +2.8 dB |

---

## Analysis

**The prediction was wrong in both magnitude and direction.**

1. **Magnitude:** The RSSNR difference is 3–4 dB, not 10–40 dB. If coherent processing were the mechanism behind the 37 dB AGASEA gap, we would need to see a similar-magnitude difference here. We see ~10× less.

2. **Direction:** Across 20 frames, RSSNR(qlook) is **3.8 dB higher** than RSSNR(standard), not lower. The coherent gain hypothesis predicts standard should be higher (more coherent integration → more benefit for specular surface). The opposite is observed.

3. **Absolute power shift is symmetric:** Standard has ~18–24 dB more surface power than qlook and ~20–22 dB more bed power. The shift is nearly equal for both targets, meaning the processing difference acts as a roughly uniform scaling factor — it does not preferentially amplify the specular surface over the diffuse bed. This is inconsistent with coherent gain differential.

4. **RSSNR standard deviation is identical (19.0 dB)** in both products, confirming the spatial variation of RSSNR is the same regardless of product — the two products are measuring the same physical quantity with a nearly constant offset.

**Conclusion: the coherent SAR processing hypothesis is refuted.** The ~37 dB AGASEA offset has a different cause.

---

## Revised explanation for the AGASEA offset

Chu et al. (2021) Section 2.2 describes AGASEA `reflectivity` as geometrically corrected bed echo power, cross-leveled between the HiCARS and PASIN systems, then attenuation-corrected. At no point is the surface return used as a normalization — the cross-system leveling is done by fitting an exponential to the difference in **bed echo power** between overlapping HiCARS and PASIN transects. Figure 3e/f in the paper shows both "initial bed power prior to any corrections" and "final relative reflectivity" as bed-power-only quantities. The final reflectivity is bed power relative to a common survey-wide mean, not relative to the received surface power.

```
reflectivity [dB] = P_bed_received
                    + 20·log10(r)          ← geometric correction (inverse square, sensor range)
                    + cross-system offset  ← exponential fit to bed power at overlapping transects
                    + 2 · atten_rate · h   ← two-way attenuation correction
                    − survey_mean          ← leveled to common mean
```

In contrast, OPR's `required_surface_snr_dB` = `P_surf − P_bed + geometry_correction` is a ratio of the two received return powers (surface-to-bed), inherently including the ~60 dB brightness excess of the ice surface over the bed.

**Consequence:** `rssnr_equiv = −(reflectivity_AGASEA − 2·N·h)` does **not** equal OPR's RSSNR. The two quantities measure fundamentally different things — `rssnr_equiv` is approximately an absolute bed reflectivity (survey-mean-referenced), while OPR RSSNR is a surface-to-bed power ratio. The ~38 dB median difference between them is not a calibration error; it reflects this definitional gap.

Only `ice_thickness` is a valid apples-to-apples comparison between the AGASEA and OPR datasets.
