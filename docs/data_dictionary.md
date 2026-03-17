# Data Dictionary
## Healthcare Staffing Analytics Pipeline

| | |
|---|---|
| **Version** | 1.0 |
| **Date** | March 2026 |
| **Author** | Fulati Paerhati |
| **Source** | CMS Payroll-Based Journal (PBJ) Q2 2024 |

---

## 1. Overview

This document defines every column across all source files used in the
pipeline. It documents data types, business definitions, known quality
issues, and transformation decisions discovered during EDA.

All findings in this document are derived from
`notebooks/01_eda_staffing.ipynb`.

---

## 2. Master File — PBJ_Daily_Nurse_Staffing_Q2_2024.csv

**Description:** Daily nurse staffing hours reported by skilled nursing
facilities to CMS via the Payroll-Based Journal system.

**Row count:** 1,325,324
**Grain:** One row per provider (`PROVNUM`) per work date (`WorkDate`)
**Date range:** 2024-04-01 → 2024-06-30 (Q2 2024, 91 days)
**Unique providers:** 14,564
**Coverage:** 100% — every provider reported every day

### 2.1 Identifier Columns

| # | Column | Source Type | Pipeline Type | Description | Notes |
|---|---|---|---|---|---|
| 0 | PROVNUM | object | STRING | CMS provider number — unique facility identifier | Has leading zeros (e.g. 015009). NEVER cast to integer. Renamed to `ccn` in Silver layer |
| 1 | PROVNAME | object | STRING | Facility name | May contain commas and special characters |
| 2 | CITY | object | STRING | City where facility is located | |
| 3 | STATE | object | STRING | Two-letter state code | 52 unique values: 50 states + DC + PR |
| 4 | COUNTY_NAME | object | STRING | County name | |
| 5 | COUNTY_FIPS | int64 | INTEGER | Federal Information Processing Standard county code | Numeric, no leading zeros issue |
| 6 | CY_Qtr | object | DROPPED | Calendar year quarter (e.g. 2024Q2) | Redundant — fully derivable from WorkDate. Dropped in Silver layer |

### 2.2 Date Column

| # | Column | Source Type | Pipeline Type | Description | Notes |
|---|---|---|---|---|---|
| 7 | WorkDate | int64 | DATE | Work date stored as integer | Stored as YYYYMMDD integer (e.g. 20240401). Cast to DATE in Silver using format `%Y%m%d`. Never store as integer downstream |

### 2.3 Census Column

| # | Column | Source Type | Pipeline Type | Description | Notes |
|---|---|---|---|---|---|
| 8 | MDScensus | int64 | INTEGER | Minimum Data Set census — number of patients on that day | 320 rows with value 0. No negative values. Used as denominator in hrs_per_patient — exclude rows where MDScensus = 0 to avoid division by zero |

### 2.4 RN Director of Nursing Hours

| # | Column | Source Type | Pipeline Type | Description | Notes |
|---|---|---|---|---|---|
| 9  | Hrs_RNDON     | float64 | DOUBLE | Total hours worked by RN Director of Nursing | 37.4% zeros |
| 10 | Hrs_RNDON_emp | float64 | DOUBLE | Hours by employed RN Director of Nursing | 38.4% zeros |
| 11 | Hrs_RNDON_ctr | float64 | DOUBLE | Hours by contracted RN Director of Nursing | 98.8% zeros — contract RNDON is rare |

### 2.5 RN Administrator Hours

| # | Column | Source Type | Pipeline Type | Description | Notes |
|---|---|---|---|---|---|
| 12 | Hrs_RNadmin     | float64 | DOUBLE | Total hours by RN Administrators | 43.7% zeros |
| 13 | Hrs_RNadmin_emp | float64 | DOUBLE | Hours by employed RN Administrators | 44.6% zeros |
| 14 | Hrs_RNadmin_ctr | float64 | DOUBLE | Hours by contracted RN Administrators | 97.2% zeros |

### 2.6 Registered Nurse Hours

| # | Column | Source Type | Pipeline Type | Description | Notes |
|---|---|---|---|---|---|
| 15 | Hrs_RN     | float64 | DOUBLE | Total hours by all Registered Nurses | 6.7% zeros. Max=915.98 (outlier flagged). Core metric column |
| 16 | Hrs_RN_emp | float64 | DOUBLE | Hours by employed RNs | 8.5% zeros |
| 17 | Hrs_RN_ctr | float64 | DOUBLE | Hours by contracted RNs | 83.9% zeros — contract RNs uncommon |

### 2.7 LPN Administrator Hours

| # | Column | Source Type | Pipeline Type | Description | Notes |
|---|---|---|---|---|---|
| 18 | Hrs_LPNadmin     | float64 | DOUBLE | Total hours by LPN Administrators | 56.9% zeros |
| 19 | Hrs_LPNadmin_emp | float64 | DOUBLE | Hours by employed LPN Administrators | 57.1% zeros |
| 20 | Hrs_LPNadmin_ctr | float64 | DOUBLE | Hours by contracted LPN Administrators | 99.4% zeros |

### 2.8 Licensed Practical Nurse Hours

| # | Column | Source Type | Pipeline Type | Description | Notes |
|---|---|---|---|---|---|
| 21 | Hrs_LPN     | float64 | DOUBLE | Total hours by all LPNs | 2.5% zeros. Max=13,946.25 (impossible — outlier flagged). Core metric column |
| 22 | Hrs_LPN_emp | float64 | DOUBLE | Hours by employed LPNs | 3.5% zeros |
| 23 | Hrs_LPN_ctr | float64 | DOUBLE | Hours by contracted LPNs | 74.3% zeros |

### 2.9 Certified Nursing Assistant Hours

| # | Column | Source Type | Pipeline Type | Description | Notes |
|---|---|---|---|---|---|
| 24 | Hrs_CNA     | float64 | DOUBLE | Total hours by all CNAs | 0.4% zeros — CNAs almost always present. Max=1,758.10 (outlier). Core metric column |
| 25 | Hrs_CNA_emp | float64 | DOUBLE | Hours by employed CNAs | 0.6% zeros |
| 26 | Hrs_CNA_ctr | float64 | DOUBLE | Hours by contracted CNAs | 70.5% zeros |

### 2.10 Nursing Assistant in Training Hours

| # | Column | Source Type | Pipeline Type | Description | Notes |
|---|---|---|---|---|---|
| 27 | Hrs_NAtrn     | float64 | DOUBLE | Total hours by Nursing Assistants in training | 81.2% zeros |
| 28 | Hrs_NAtrn_emp | float64 | DOUBLE | Hours by employed NAs in training | 81.3% zeros |
| 29 | Hrs_NAtrn_ctr | float64 | DOUBLE | Hours by contracted NAs in training | 99.8% zeros |

### 2.11 Medication Aide Hours

| # | Column | Source Type | Pipeline Type | Description | Notes |
|---|---|---|---|---|---|
| 30 | Hrs_MedAide     | float64 | DOUBLE | Total hours by Medication Aides | 69.2% zeros. Max=429.80 (outlier flagged) |
| 31 | Hrs_MedAide_emp | float64 | DOUBLE | Hours by employed Medication Aides | 69.5% zeros |
| 32 | Hrs_MedAide_ctr | float64 | DOUBLE | Hours by contracted Medication Aides | 98.4% zeros |

### 2.12 Derived Columns (added in Silver layer)

| Column | Type | Formula | Description |
|---|---|---|---|
| `ccn` | STRING | renamed from PROVNUM | Standardized provider identifier |
| `work_date` | DATE | cast from WorkDate | Proper date type |
| `month` | INTEGER | extract from work_date | Month number (4, 5, 6) |
| `week` | INTEGER | extract from work_date | ISO week number |
| `day_of_week` | STRING | extract from work_date | Monday through Sunday |
| `is_weekend` | BOOLEAN | day_of_week IN (Saturday, Sunday) | Weekend flag |
| `total_nursing_hrs` | DOUBLE | Hrs_RN + Hrs_LPN + Hrs_CNA | Total direct care nursing hours |
| `hrs_per_patient` | DOUBLE | total_nursing_hrs / MDScensus | Nurse hours per patient per day — primary metric |
| `is_ghost_row` | BOOLEAN | MDScensus=0 AND total_nursing_hrs>0 | 165 rows — census zero but staff worked |
| `is_outlier` | BOOLEAN | any key Hrs_ column exceeds IQR upper bound | Statistically extreme values |
| `_ingested_at` | TIMESTAMP | pipeline metadata | When this record was ingested |
| `_source_file` | STRING | pipeline metadata | Source filename |
| `_batch_id` | STRING | pipeline metadata | UUID for the ingestion run |

---

## 3. NH_ProviderInfo_Oct2024.csv

**Description:** Provider metadata including facility characteristics,
bed counts, star ratings, and contact information.

**Row count:** 14,814
**Join key:** `CMS Certification Number (CCN)` → renamed to `ccn`
**Match rate:** 99.9% (14,547 of 14,564 master providers)
**Null rate:** 15.2% overall
**17 unmatched providers:** Flagged as `is_provider_info_missing = True`

### Key Columns Used in Pipeline

| Column | Type | Description | Notes |
|---|---|---|---|
| CMS Certification Number (CCN) | object | Provider identifier | Renamed to `ccn` in Silver |
| Provider Name | object | Facility name | |
| Provider Address | object | Street address | |
| City/Town | object | City | |
| State | object | State code | |
| ZIP Code | object | ZIP code | |
| Number of Certified Beds | float64 | Licensed bed capacity | Used for bed utilization metric |
| Overall Rating | float64 | CMS 5-star overall rating | 1-5 scale, nulls where not rated |
| Health Inspection Rating | float64 | Star rating for inspections | |
| Staffing Rating | float64 | Star rating for staffing | |
| RN Staffing Rating | float64 | Star rating for RN staffing specifically | |
| Ownership Type | object | For-profit / Non-profit / Government | |
| Provider Type | object | Facility classification | Used to filter/group by hospital type |

> **Note:** This file has 103 columns total. Only the above columns
> are extracted in the Silver transformation. All others are dropped.

---

## 4. NH_Penalties_Oct2024.csv

**Description:** Financial penalties and payment denials issued to
skilled nursing facilities by CMS.

**Row count:** 28,505
**Join key:** `CMS Certification Number (CCN)` → renamed to `ccn`
**Match rate:** 62.4% (9,093 of 14,564 master providers)
**Null rate:** 14.5%

> **Important:** 37.6% of facilities have NO penalties. This is not
> missing data — it means those facilities have a clean record.
> Always use LEFT JOIN when joining to fact table.

### Key Columns Used in Pipeline

| Column | Type | Description | Notes |
|---|---|---|---|
| CMS Certification Number (CCN) | object | Provider identifier | Renamed to `ccn` |
| Penalty Date | object | Date penalty was issued | Cast to DATE in Silver |
| Penalty Type | object | Fine vs Payment Denial | |
| Fine Amount | float64 | Dollar amount of fine | Null for payment denials |
| Payment Denial Start Date | object | Start of payment denial period | |
| Payment Denial End Date | object | End of payment denial period | |

---

## 5. NH_QualityMsr_Claims_Oct2024.csv

**Description:** Quality measures derived from Medicare claims data,
including readmission rates and emergency visit rates per facility.

**Row count:** 59,256
**Join key:** `CMS Certification Number (CCN)` → renamed to `ccn`
**Match rate:** 99.9% (14,547 of 14,564 master providers)
**Null rate:** 8.3%
**Format:** LONG — one row per provider per measure code

> **Important:** This file is in long format. Each facility has
> multiple rows — one per quality measure. Must be pivoted wide
> in the dbt intermediate layer before joining to fact table.

### Key Columns Used in Pipeline

| Column | Type | Description | Notes |
|---|---|---|---|
| CMS Certification Number (CCN) | object | Provider identifier | Renamed to `ccn` |
| Measure Code | object | Quality measure identifier | Used as pivot column |
| Measure Description | object | Human-readable measure name | |
| Score | float64 | Measure value for this facility | 8.3% nulls — facility not rated |
| Footnote | object | Explanation for missing scores | |

### Key Measure Codes to Extract

| Measure Code | Description | Used For |
|---|---|---|
| SNF_30DAY_READM_RATE | 30-day readmission rate | Readmission metric |
| SNF_ER_VISIT_RATE | Emergency room visit rate | Quality outcome |

---

## 6. FY_2024_SNF_VBP_Facility_Performance.csv

**Description:** Value-Based Purchasing program performance scores
and incentive payment information for skilled nursing facilities.

**Row count:** 10,858
**Join key:** `CMS Certification Number (CCN)` → renamed to `ccn`
**Match rate:** 63.9% (9,309 of 14,564 master providers)
**Null rate:** 34.5%

> **Important:** 36.1% of facilities do not participate in VBP
> program (typically smaller facilities are exempt). Non-participants
> will have null scores — flag as `is_vbp_participant = False`.
> Always use LEFT JOIN.

### Key Columns Used in Pipeline

| Column | Type | Description | Notes |
|---|---|---|---|
| CMS Certification Number (CCN) | object | Provider identifier | Renamed to `ccn` |
| SNF VBP Program Ranking | float64 | National ranking in VBP program | Null if not participating |
| Performance Score | float64 | VBP performance score | Null if not participating |
| Incentive Payment Multiplier | float64 | Payment adjustment factor | >1.0 = bonus, <1.0 = penalty |

---

## 7. Data Quality Summary

### Master File Quality Flags

| Flag | Column | Count | % of Total | Action |
|---|---|---|---|---|
| `is_ghost_row` | MDScensus=0 AND hrs>0 | 165 | 0.01% | Flag, keep, investigate |
| `is_outlier` | Hrs_LPN > IQR upper bound | ~42,389 | 3.20% | Flag, keep, investigate |
| `is_outlier` | Hrs_RN > IQR upper bound | ~64,542 | 4.87% | Flag, keep, investigate |
| `is_outlier` | Hrs_CNA > IQR upper bound | ~49,416 | 3.73% | Flag, keep, investigate |
| `is_outlier` | Hrs_MedAide > IQR upper bound | ~157,330 | 11.87% | Flag, keep, investigate |

### Join Coverage Summary

| Dimension Table | Match Rate | Unmatched | Handling |
|---|---|---|---|
| dim_provider | 99.9% | 17 providers | Flag `is_provider_info_missing` |
| dim_penalties | 62.4% | ~5,471 providers | No penalties = valid, coalesce to 0 |
| dim_quality_claims | 99.9% | 17 providers | Null scores = not rated |
| dim_vbp_performance | 63.9% | ~5,255 providers | Not participating = valid, flag |

### Golden Rules for This Dataset

```
1. NEVER cast PROVNUM/ccn to integer — leading zeros will be lost
2. NEVER divide by MDScensus without filtering MDScensus > 0 first
3. NEVER drop rows with quality flags — flag and keep always
4. ALWAYS use LEFT JOIN for dimension tables — unmatched is valid
5. ZEROS in _ctr columns are REAL ZEROS — not missing data
6. WorkDate MUST be cast from int64 to DATE before any date math
7. NH_QualityMsr_Claims MUST be pivoted wide before joining
```

---

## 8. Skipped Files Reference

| File | Reason Skipped |
|---|---|
| NH_QualityMsr_MDS_Oct2024.csv | 21.7% nulls, complex pivoting — v2 backlog |
| NH_SurveySummary_Oct2024.csv | Good v2 candidate, not critical for v1 |
| NH_Ownership_Oct2024.csv | Multiple rows per facility, deduplication needed |
| NH_HealthCitations_Oct2024.csv | Out of scope for staffing metrics |
| NH_FireSafetyCitations_Oct2024.csv | Out of scope — fire safety unrelated |
| NH_CovidVaxProvider_20241027.csv | Out of scope for staffing metrics |
| NH_CovidVaxAverages_20241027.csv | State-level only, no facility detail |
| NH_SurveyDates_Oct2024.csv | NH_SurveySummary covers same ground |
| NH_CitationDescriptions_Oct2024.csv | Lookup only, not needed for metrics |
| NH_HlthInspecCutpointsState_Oct2024.csv | Metadata only |
| NH_DataCollectionIntervals_Oct2024.csv | Metadata only |
| NH_StateUSAverages_Oct2024.csv | 54 rows — use as dbt seed if needed |
| FY_2024_SNF_VBP_Aggregate_Performance.csv | 1 row — national aggregate only |
| Skilled_Nursing_QRP_Provider_Data_Oct2024.csv | Overlaps with NH_QualityMsr_Claims |
| Skilled_Nursing_QRP_National_Data_Oct2024.csv | National only, no facility detail |
| Swing_Bed_SNF_data_Oct2024.csv | Different facility type, out of scope |
