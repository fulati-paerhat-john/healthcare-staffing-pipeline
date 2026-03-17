# Metrics Definition Document
## Healthcare Staffing Analytics Pipeline

| | |
|---|---|
| **Version** | 1.0 |
| **Date** | March 2026 |
| **Author** | Fulati Paerhati |

---

## 1. Overview

This document defines every metric considered for this project.
For each metric it documents: the formula, source columns, which
dbt mart it lives in, and — critically — whether it is calculable
from the available CMS PBJ data.

The CMS PBJ dataset tracks **aggregated daily hours per facility**.
It does not contain individual nurse records, wage data, or intra-day
timestamps. This constrains which metrics are possible.

> The project brief explicitly states: *"You don't have data to solve
> all the below metrics. You need to understand the data and identify
> which among them are calculatable and calculate those."*
> This document is the formal answer to that requirement.

---

## 2. Calculable Metrics (Built in This Project)

### Metric 1 — Nurse Hours per Patient per Day

| | |
|---|---|
| **Category** | Staffing |
| **Business Question** | Are facilities providing adequate staffing per patient? |
| **Formula** | `(Hrs_RN + Hrs_LPN + Hrs_CNA) / MDScensus` |
| **Grain** | Per facility per day |
| **Source Columns** | Hrs_RN, Hrs_LPN, Hrs_CNA, MDScensus |
| **Source File** | PBJ_Daily_Nurse_Staffing_Q2_2024.csv |
| **Filter** | MDScensus > 0 (exclude rows where census is zero) |
| **dbt Mart** | mart_staffing_daily, mart_staffing_by_state, mart_facility_summary |
| **Aggregations** | By facility, by state, by month |
| **Benchmark** | CMS proposed minimum: 3.48 hrs/patient/day |
| **EDA Finding** | National mean = 3.37 — BELOW CMS minimum |

```sql
-- Core calculation
SELECT
    ccn,
    work_date,
    state,
    mdscensus,
    hrs_rn + hrs_lpn + hrs_cna                           AS total_nursing_hrs,
    (hrs_rn + hrs_lpn + hrs_cna) / NULLIF(mdscensus, 0)  AS hrs_per_patient,
    CASE WHEN (hrs_rn + hrs_lpn + hrs_cna) / NULLIF(mdscensus, 0) < 3.48
         THEN TRUE ELSE FALSE END                         AS below_cms_minimum
FROM silver.fact_daily_staffing
WHERE mdscensus > 0
  AND is_ghost_row = FALSE
  AND is_outlier = FALSE
```

---

### Metric 2 — Total Staffing Hours by Hospital, State, Month

| | |
|---|---|
| **Category** | Staffing |
| **Business Question** | How many total nursing hours are worked where and when? |
| **Formula** | `SUM(all Hrs_ columns)` grouped by dimension |
| **Grain** | Per facility per month, per state per month |
| **Source Columns** | All Hrs_ columns |
| **Source File** | PBJ_Daily_Nurse_Staffing_Q2_2024.csv |
| **dbt Mart** | mart_staffing_by_state, mart_facility_summary |

```sql
SELECT
    ccn,
    state,
    month,
    SUM(hrs_rn)       AS total_rn_hrs,
    SUM(hrs_lpn)      AS total_lpn_hrs,
    SUM(hrs_cna)      AS total_cna_hrs,
    SUM(hrs_medaide)  AS total_medaide_hrs,
    SUM(hrs_rn + hrs_lpn + hrs_cna + hrs_medaide
        + hrs_natrn + hrs_rndon + hrs_rnadmin
        + hrs_lpnadmin) AS total_all_hrs
FROM silver.fact_daily_staffing
GROUP BY ccn, state, month
```

---

### Metric 3 — Bed Utilization Rate

| | |
|---|---|
| **Category** | Facility |
| **Business Question** | How full are facilities relative to their licensed capacity? |
| **Formula** | `MDScensus / number_of_certified_beds` |
| **Grain** | Per facility per day, aggregated monthly |
| **Source Columns** | MDScensus (master), Number of Certified Beds (NH_ProviderInfo) |
| **Source Files** | Master file + NH_ProviderInfo_Oct2024.csv |
| **dbt Mart** | mart_facility_summary |
| **Caveat** | Q2 2024 only — cannot show trends over full year |

```sql
SELECT
    f.ccn,
    f.work_date,
    f.mdscensus,
    p.number_of_certified_beds,
    f.mdscensus / NULLIF(p.number_of_certified_beds, 0) AS bed_utilization_rate
FROM silver.fact_daily_staffing f
LEFT JOIN silver.dim_provider p ON f.ccn = p.ccn
WHERE p.number_of_certified_beds > 0
```

---

### Metric 4 — Staffing Levels vs Bed Occupancy Comparison

| | |
|---|---|
| **Category** | Facility |
| **Business Question** | Do facilities with higher occupancy maintain adequate staffing? |
| **Formula** | Scatter: `bed_utilization_rate` vs `hrs_per_patient` |
| **Grain** | Per facility (Q2 average) |
| **Source Columns** | MDScensus, Hrs_RN, Hrs_LPN, Hrs_CNA, certified_beds |
| **Source Files** | Master file + NH_ProviderInfo |
| **dbt Mart** | mart_facility_summary |

```sql
SELECT
    ccn,
    AVG(mdscensus / NULLIF(certified_beds, 0))              AS avg_occupancy_rate,
    AVG((hrs_rn + hrs_lpn + hrs_cna) / NULLIF(mdscensus,0)) AS avg_hrs_per_patient
FROM mart_facility_summary
GROUP BY ccn
```

---

### Metric 5 — Top 10 Hospitals by Patient Throughput

| | |
|---|---|
| **Category** | Facility |
| **Business Question** | Which facilities serve the most patients? |
| **Formula** | `AVG(MDScensus)` ranked descending |
| **Grain** | Per facility over Q2 2024 |
| **Source Columns** | MDScensus, PROVNAME, STATE |
| **Source File** | Master file |
| **dbt Mart** | mart_facility_summary |

```sql
SELECT
    ccn,
    provname,
    state,
    AVG(mdscensus) AS avg_daily_census,
    MAX(mdscensus) AS peak_census
FROM silver.fact_daily_staffing
GROUP BY ccn, provname, state
ORDER BY avg_daily_census DESC
LIMIT 10
```

---

### Metric 6 — Facilities with Lowest Staffing vs Patient Load

| | |
|---|---|
| **Category** | Facility |
| **Business Question** | Which facilities are most understaffed relative to their patients? |
| **Formula** | `hrs_per_patient` ranked ascending |
| **Grain** | Per facility (Q2 average) |
| **Source Columns** | Hrs_RN, Hrs_LPN, Hrs_CNA, MDScensus |
| **Source File** | Master file |
| **dbt Mart** | mart_cms_compliance |

```sql
SELECT
    ccn,
    provname,
    state,
    AVG((hrs_rn + hrs_lpn + hrs_cna) / NULLIF(mdscensus,0)) AS avg_hrs_per_patient,
    CASE WHEN AVG((hrs_rn + hrs_lpn + hrs_cna) /
              NULLIF(mdscensus,0)) < 3.48
         THEN TRUE ELSE FALSE END AS below_cms_minimum
FROM silver.fact_daily_staffing
WHERE mdscensus > 0
GROUP BY ccn, provname, state
ORDER BY avg_hrs_per_patient ASC
```

---

### Metric 7 — Readmission Rates by Hospital and State

| | |
|---|---|
| **Category** | Quality |
| **Business Question** | Which facilities have the highest patient readmission rates? |
| **Formula** | SNF_30DAY_READM_RATE from NH_QualityMsr_Claims |
| **Grain** | Per facility (period average), per state |
| **Source Columns** | Measure Code = SNF_30DAY_READM_RATE, Score |
| **Source File** | NH_QualityMsr_Claims_Oct2024.csv |
| **dbt Mart** | mart_quality (intermediate pivot then mart) |
| **Caveat** | No diagnosis category breakdown available |

```sql
SELECT
    ccn,
    state,
    score AS readmission_rate_30day
FROM silver.dim_quality_claims
WHERE measure_code = 'SNF_30DAY_READM_RATE'
  AND score IS NOT NULL
```

---

### Metric 8 — Staffing vs Readmission Rate Correlation

| | |
|---|---|
| **Category** | Quality |
| **Business Question** | Does higher staffing correlate with lower readmission rates? |
| **Formula** | Pearson correlation: `hrs_per_patient` vs `readmission_rate` |
| **Grain** | Per facility (Q2 average) |
| **Source Files** | Master file + NH_QualityMsr_Claims |
| **dbt Mart** | mart_facility_summary (joined) |
| **Note** | Correlation, not causation — document this caveat in dashboard |

```sql
SELECT
    CORR(avg_hrs_per_patient, readmission_rate_30day) AS staffing_readmission_correlation
FROM mart_facility_summary
WHERE avg_hrs_per_patient IS NOT NULL
  AND readmission_rate_30day IS NOT NULL
```

---

### Metric 9 — Permanent vs Contract Staff Ratio

| | |
|---|---|
| **Category** | Operational |
| **Business Question** | How reliant are facilities on contract/agency staff? |
| **Formula** | `SUM(_ctr cols) / (SUM(_emp cols) + SUM(_ctr cols))` |
| **Grain** | Per facility per month |
| **Source Columns** | All _emp and _ctr column pairs |
| **Source File** | Master file |
| **dbt Mart** | mart_facility_summary |
| **EDA Finding** | National contract ratio: LPN 9.3%, RN 8.1%, CNA 7.0% |

```sql
SELECT
    ccn,
    month,
    SUM(hrs_rn_ctr + hrs_lpn_ctr + hrs_cna_ctr)  AS total_contract_hrs,
    SUM(hrs_rn_emp + hrs_lpn_emp + hrs_cna_emp)  AS total_employed_hrs,
    SUM(hrs_rn_ctr + hrs_lpn_ctr + hrs_cna_ctr) /
    NULLIF(SUM(hrs_rn_ctr + hrs_lpn_ctr + hrs_cna_ctr +
               hrs_rn_emp + hrs_lpn_emp + hrs_cna_emp), 0)
    AS contract_ratio_pct
FROM silver.fact_daily_staffing
GROUP BY ccn, month
```

---

### Metric 10 — CMS Compliance Flag

| | |
|---|---|
| **Category** | Staffing |
| **Business Question** | Which facilities are below the proposed CMS minimum? |
| **Formula** | `hrs_per_patient < 3.48` |
| **Threshold** | 3.48 hrs/patient/day (CMS proposed minimum, 2024) |
| **Grain** | Per facility (Q2 average) |
| **Source Columns** | Hrs_RN, Hrs_LPN, Hrs_CNA, MDScensus |
| **Source File** | Master file |
| **dbt Mart** | mart_cms_compliance |
| **EDA Finding** | National mean 3.37 — majority of facilities below threshold |

```sql
SELECT
    ccn,
    provname,
    state,
    AVG((hrs_rn + hrs_lpn + hrs_cna) / NULLIF(mdscensus,0)) AS avg_hrs_per_patient,
    CASE WHEN AVG((hrs_rn + hrs_lpn + hrs_cna) /
              NULLIF(mdscensus,0)) < 3.48
         THEN 'Non-compliant'
         ELSE 'Compliant' END                              AS cms_compliance_status
FROM silver.fact_daily_staffing
WHERE mdscensus > 0
  AND is_ghost_row = FALSE
GROUP BY ccn, provname, state
```

---

## 3. Partially Calculable Metrics

### Occupancy Rate Trends

| | |
|---|---|
| **Status** | ⚠️ Partial |
| **Limitation** | Q2 2024 only — cannot show trends over full year |
| **What we can show** | Monthly trend April → May → June 2024 |
| **What we cannot show** | Year-over-year or multi-quarter trends |

### Readmission by Diagnosis Category

| | |
|---|---|
| **Status** | ⚠️ Partial |
| **Limitation** | NH_QualityMsr_Claims has overall readmission rate only |
| **What we can show** | Readmission rate per facility and state |
| **What we cannot show** | Breakdown by diagnosis — not in any available file |

---

## 4. Not Calculable Metrics

The following metrics were requested in the project brief but cannot
be calculated from the available CMS PBJ data. The reason for each
is documented below.

### 4.1 Staffing Metrics — Not Calculable

| Metric | Reason Not Calculable |
|---|---|
| % nurses working overtime | CMS PBJ data is aggregated daily hours per facility. There are no individual nurse records, shift records, or hourly breakdowns. Overtime requires knowing individual hours per nurse per shift |
| Number of shifts per nurse | Same reason — no individual nurse tracking. A nurse with 12 hours logged could have worked one shift or two. This granularity does not exist in CMS PBJ |
| Avg and median shifts per nurse | Same reason as above |

### 4.2 Facility Metrics — Not Calculable

| Metric | Reason Not Calculable |
|---|---|
| Occupancy rate over past year | Dataset covers Q2 2024 only (April–June). CMS PBJ is released quarterly — prior quarters would require separate downloads |
| Bed utilization by department | No department column exists in any CMS file. All data is at the facility level only |

### 4.3 Quality Metrics — Not Calculable

| Metric | Reason Not Calculable |
|---|---|
| Patient satisfaction scores | Not published in CMS PBJ or any of the 21 supporting files available |
| Average length of stay (ALOS) | No admission or discharge date in any file. ALOS requires patient-level records which CMS PBJ does not publish |
| Patient-to-nurse complaint ratio | No complaint data in any CMS file |
| Readmission by diagnosis category | NH_QualityMsr_Claims provides overall rate only, not broken down by diagnosis |

### 4.4 Cost Metrics — Not Calculable

| Metric | Reason Not Calculable |
|---|---|
| Total payroll costs | CMS PBJ tracks hours worked only. No wage or salary data is published in any public CMS dataset |
| Average cost per patient stay | No cost or revenue data in any file |
| Overtime cost as % of payroll | No wage data available (and individual overtime hours also not available) |
| Hospital revenue vs payroll | Proprietary financial data — not in any public CMS dataset |

> **Note:** All cost data is considered proprietary by healthcare
> facilities. CMS does not require facilities to report wages or
> revenue in the PBJ system. This entire category is structurally
> out of scope for any project using public CMS data only.

### 4.5 Operational Metrics — Not Calculable

| Metric | Reason Not Calculable |
|---|---|
| Shift utilization by time of day | CMS PBJ data is daily totals only. No time-of-day or shift breakdown exists in the data |
| Peak staffing hours per hospital | Same reason — no intra-day granularity |
| Nurse attrition rate | No individual nurse tracking across time periods. CMS PBJ does not identify individual nurses |

---

## 5. Metrics Coverage Summary

| Category | Requested | Calculable | Partial | Not Calculable |
|---|---|---|---|---|
| Staffing | 4 | 2 | 0 | 2 |
| Facility | 5 | 4 | 1 | 1 |
| Quality | 5 | 2 | 1 | 3 |
| Cost | 4 | 0 | 0 | 4 |
| Operational | 4 | 1 | 0 | 3 |
| **Total** | **22** | **10** | **2** | **13** |

---

## 6. dbt Mart Mapping

| Mart Table | Metrics Served | Grain |
|---|---|---|
| `mart_staffing_daily` | Metric 1, 2, 9 | PROVNUM × DATE |
| `mart_staffing_by_state` | Metric 1, 2 | STATE × MONTH |
| `mart_facility_summary` | Metric 3, 4, 5, 7, 8, 9 | PROVNUM (Q2 avg) |
| `mart_cms_compliance` | Metric 6, 10 | PROVNUM |
| `mart_penalty_correlation` | Metrics 1, 6 + penalty data | PROVNUM |
