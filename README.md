# 🏥 Healthcare Staffing Analytics Pipeline

> An end-to-end, production-grade data pipeline and analytics dashboard
> for hospital nurse staffing and operational performance metrics
> across US skilled nursing facilities — built on a modern lakehouse architecture.

---

## 📌 Problem Statement

Healthcare networks lack a unified view of staffing efficiency, nurse-to-patient
ratios, and operational performance across facilities. Fragmented data across
providers, states, and time periods makes it difficult for management to:

- Identify understaffed or overburdened facilities
- Understand how staffing levels affect patient outcomes
- Optimize workforce planning and control overtime costs
- Benchmark facilities against state and national averages
- Track CMS compliance thresholds across the network

This project ingests CMS Payroll-Based Journal (PBJ) staffing data alongside
supporting datasets to build a scalable analytics platform that surfaces
these insights through an interactive dashboard.

---

## 🏗️ Architecture

> Architecture diagram — added after Phase 2 design document

**Pipeline Flow:**

```
Google Drive (Source CSVs)
        ↓
AWS S3 — Bronze Layer (raw, immutable, partitioned by state/date)
        ↓
PySpark on Databricks — Silver Layer (cleaned, typed, flagged)
        ↓
Snowflake + dbt — Gold Layer (metrics, marts, tests)
        ↓
Streamlit + Plotly — Serving Layer (interactive dashboard)
```

**Orchestration:** Apache Airflow (Docker locally → MWAA in production)
**Infrastructure:** Terraform (all AWS + Snowflake resources as code)
**CI/CD:** GitHub Actions (dbt tests + Terraform plan on every PR)

---

## 🛠️ Tech Stack

| Layer | Tool | Why This Tool |
|---|---|---|
| Ingestion | Python + Google Drive API | Incremental extraction from source |
| Raw Storage | AWS S3 (Bronze/Silver/Gold) | Scalable, durable, cost-effective data lake |
| Processing | PySpark + Databricks | Distributed transformation at scale |
| Warehouse | Snowflake | Industry standard for analytics + dbt's native home |
| Transformation | dbt Core | Data lineage, testing, documentation, modularity |
| Orchestration | Apache Airflow (Docker) | Most widely adopted pipeline orchestrator |
| IaC | Terraform | Reproducible, version-controlled infrastructure |
| Dashboard | Streamlit + Plotly | Fast Python-native interactive visualization |
| CI/CD | GitHub Actions | Automated testing and validation on every push |
| Code Quality | black + flake8 + sqlfluff | Consistent formatting via pre-commit hooks |

---

## 📁 Project Structure

```
healthcare-staffing-pipeline/
│
├── .github/
│   └── workflows/
│       ├── dbt_test.yml              # Run dbt tests on every PR
│       └── terraform_plan.yml        # Terraform plan on every PR
│
├── infra/                            # Terraform — all infrastructure as code
│   ├── main.tf
│   ├── variables.tf
│   ├── s3.tf                         # Bronze / Silver / Gold S3 buckets
│   ├── iam.tf                        # Least-privilege IAM roles
│   └── snowflake.tf                  # Snowflake DB, schema, warehouse
│
├── ingestion/                        # Bronze layer — raw data extraction
│   ├── gdrive_extractor.py           # Google Drive → S3
│   ├── schemas.py                    # Source schema definitions
│   └── utils.py
│
├── spark_jobs/                       # Silver layer — PySpark transformations
│   ├── transform_staffing.py
│   ├── transform_providers.py
│   └── job_config.yaml
│
├── dbt/                              # Gold layer — metrics and marts
│   ├── dbt_project.yml
│   ├── profiles.yml.example
│   ├── models/
│   │   ├── staging/                  # 1:1 with sources, light casting
│   │   ├── intermediate/             # Business logic and joins
│   │   └── marts/                    # Final metrics tables
│   ├── tests/
│   ├── macros/
│   └── docs/
│
├── airflow/                          # Orchestration
│   ├── docker-compose.yml
│   └── dags/
│       ├── healthcare_pipeline.py    # Master DAG
│       └── utils/
│
├── dashboard/                        # Streamlit app
│   ├── app.py
│   ├── pages/
│   │   ├── 01_staffing_overview.py
│   │   ├── 02_facility_drilldown.py
│   │   └── 03_risk_flags.py
│   ├── components/
│   └── connectors/
│       └── snowflake_conn.py
│
├── notebooks/                        # EDA only — never in pipeline path
│   └── 01_eda_staffing.ipynb
│
├── tests/                            # Unit tests for Python code
│   └── test_transform_staffing.py
│
├── docs/
│   ├── architecture.md               # Solution design document
│   ├── data_dictionary.md            # Column definitions and EDA findings
│   ├── metrics_definition.md         # Metric formulas + data availability
│   └── sme_approval.md              # SME sign-off document
│
├── .env.example
├── .gitignore
├── .pre-commit-config.yaml
├── .python-version                   # Pins Python 3.11.9 via pyenv
├── Makefile
├── pyproject.toml
├── poetry.lock
└── README.md
```

---

## 📊 Source Data

### Master Dataset

| File | Description | Rows | Grain |
|---|---|---|---|
| `PBJ_Daily_Nurse_Staffing_Q2_2024.csv` | Daily nurse staffing hours by provider — Q2 2024 | 1,325,324 | PROVNUM × WorkDate |

### Supporting Datasets — Used in Pipeline (v1)

| File | Description | Rows | Match Rate |
|---|---|---|---|
| `NH_ProviderInfo_Oct2024.csv` | Provider metadata, bed count, star ratings | 14,814 | 99.9% |
| `NH_Penalties_Oct2024.csv` | Financial penalties per facility | 28,505 | 62.4% |
| `NH_QualityMsr_Claims_Oct2024.csv` | Claims-based quality measures, readmission rates | 59,256 | 99.9% |
| `FY_2024_SNF_VBP_Facility_Performance.csv` | Value-based purchasing scores and rankings | 10,858 | 63.9% |

### Supporting Datasets — Backlog (v2)

| File | Description | Reason Deferred |
|---|---|---|
| `NH_QualityMsr_MDS_Oct2024.csv` | Clinical assessment quality measures | High null rate, complex pivoting work |
| `NH_SurveySummary_Oct2024.csv` | Inspection cycle results per facility | Good v2 candidate, not critical for v1 |
| `NH_Ownership_Oct2024.csv` | Facility ownership history | Requires deduplication across ownership rows |
| `Skilled_Nursing_QRP_Provider_Data_Oct2024.csv` | Extended quality reporting program data | Overlaps significantly with v1 quality file |

---

## 🗄️ Data Model

```
FACT TABLE
└── fact_daily_staffing          1,325,324 rows   PROVNUM × WorkDate

DIMENSION TABLES (all LEFT JOIN on ccn)
├── dim_provider                    14,814 rows   one row per facility
├── dim_penalties                   28,505 rows   many penalties per facility
├── dim_quality_claims              59,256 rows   many measures per facility
└── dim_vbp_performance             10,858 rows   one row per facility

MARTS (built by dbt on top of dims + fact)
├── mart_staffing_daily             PROVNUM × DATE
├── mart_staffing_by_state          STATE × MONTH
├── mart_facility_summary           PROVNUM (Q2 aggregated)
├── mart_cms_compliance             facilities vs 3.48 hr/patient threshold
└── mart_penalty_correlation        staffing level vs penalty amount
```

> **Key Join Discovery from EDA:** Master file uses `PROVNUM`, all supporting
> files use `CMS Certification Number (CCN)` — same identifier, different column
> names across CMS datasets. Standardized to `ccn` in the Silver layer.

---

## 🔍 Key EDA Findings

| Finding | Detail | Pipeline Action |
|---|---|---|
| Grain confirmed | PROVNUM × WorkDate 100% unique | No deduplication needed |
| WorkDate is integer | Stored as `20240401` not a date type | Cast to DATE in Silver (`%Y%m%d`) |
| PROVNUM has leading zeros | `015009` loses zero if cast to int | Always keep as VARCHAR(10) |
| Zero nulls | No missing values anywhere in master | No null imputation needed |
| 165 ghost rows | Census=0 but hours>0 | Flag `is_ghost_row` in Silver, never drop |
| Extreme outliers | Hrs_LPN max=13,946 (physically impossible) | Flag `is_outlier` via IQR in Silver |
| Weekend effect | Staffing drops every 7 days systematically | Surface daily granularity in dashboard |
| Below CMS minimum | National mean 3.37 < 3.48 hr/patient threshold | CMS compliance mart in dbt |
| Contract ratio low | 7-9% across all roles | Operational risk metric in dashboard |

---

## 📈 Metrics Assessment

> The CMS PBJ dataset tracks **aggregated daily hours per facility** — not
> individual nurse records, wages, or intra-day shifts. This shapes which
> metrics are calculable. Full breakdown in `docs/metrics_definition.md`.

### ✅ Calculable — Built in This Project

| # | Metric | Category | Formula / Source |
|---|---|---|---|
| 1 | Nurse hours per patient per day | Staffing | `(Hrs_RN + Hrs_LPN + Hrs_CNA) / MDScensus` |
| 2 | Total hours by hospital, state, month | Staffing | Sum all `Hrs_` cols, group by PROVNUM/STATE/month |
| 3 | Bed utilization rate | Facility | `MDScensus / number_of_certified_beds` (NH_ProviderInfo) |
| 4 | Staffing levels vs bed occupancy | Facility | Join hrs_per_patient with occupancy rate |
| 5 | Top 10 hospitals by patient throughput | Facility | Rank by avg `MDScensus` descending |
| 6 | Facilities with lowest staffing vs load | Facility | Rank by `hrs_per_patient` ascending |
| 7 | Readmission rates by hospital and state | Quality | `NH_QualityMsr_Claims` joined on CCN |
| 8 | Staffing vs readmission correlation | Quality | Pearson correlation: hrs_per_patient vs readmission_rate |
| 9 | Permanent vs contract staff ratio | Operational | `sum(_ctr_cols) / sum(_emp_cols)` per facility |
| 10 | CMS compliance flag | Staffing | Flag facilities where hrs_per_patient < 3.48 |

### ⚠️ Partially Calculable — Built With Caveats

| Metric | Category | Limitation |
|---|---|---|
| Occupancy rate trends | Facility | Q2 2024 only — full year data not available |
| Readmission by diagnosis | Quality | Diagnosis category breakdown not in dataset |

### ❌ Not Calculable — Data Not Available

| Metric | Category | Reason |
|---|---|---|
| % nurses working overtime | Staffing | No individual nurse records — data is facility-day aggregates |
| Shifts per nurse | Staffing | No individual nurse tracking in CMS PBJ data |
| Department-level metrics | All | No department column exists in any source file |
| Patient satisfaction scores | Quality | Not published in CMS PBJ or supporting files |
| Average length of stay (ALOS) | Quality | No admission or discharge dates in any file |
| Patient-to-nurse complaint ratio | Quality | No complaint data in any file |
| Total payroll costs | Cost | CMS PBJ tracks hours only — no wage or salary data |
| Cost per patient stay | Cost | No financial data in any CMS file used |
| Overtime cost as % of payroll | Cost | No wage data available |
| Hospital revenue vs expenses | Cost | Proprietary financial data — not in public CMS dataset |
| Shift utilization by time of day | Operational | Daily totals only — no intra-day time breakdown |
| Peak staffing hours | Operational | Same — no time-of-day granularity in source |
| Nurse attrition rate | Operational | No individual nurse tracking across time periods |

---

## ❓ Project Questions — Answer Plan

| Question | Data Source | Delivered In |
|---|---|---|
| Relationship between staffing and occupancy? | mart_facility_summary | Dashboard Page 2 |
| Hospitals with highest overtime hours? | mart_staffing_daily | Dashboard Page 3 |
| Average staffing by state and hospital type? | mart_staffing_by_state + dim_provider | Dashboard Page 1 |
| Trends in patient length of stay? | NH_QualityMsr_Claims | Dashboard Page 2 |

---

## 🚀 Getting Started

### Prerequisites

- Python 3.11.9 (managed via pyenv)
- Poetry 2.0+
- Docker Desktop
- AWS CLI v2 (configured with named profile)
- Terraform 1.7+

### Installation

```bash
# Clone the repo
git clone https://github.com/your-username/healthcare-staffing-pipeline.git
cd healthcare-staffing-pipeline

# Pin Python version (pyenv reads .python-version automatically)
pyenv local 3.11.9

# Install all dependencies
poetry install

# Activate virtual environment
eval $(poetry env activate)

# Install pre-commit hooks
pre-commit install

# Copy environment variables template and fill in your values
cp .env.example .env
```

### AWS Configuration

```bash
# Configure named profile — never use root or default
aws configure --profile healthcare-pipeline

# Verify credentials
aws sts get-caller-identity --profile healthcare-pipeline
```

### Infrastructure Provisioning

```bash
cd infra
terraform init
terraform plan
terraform apply
```

### Running the Pipeline

```bash
# Start Airflow
cd airflow
docker-compose up -d

# Run dbt models
cd dbt
dbt run
dbt test

# Launch dashboard
cd dashboard
streamlit run app.py
```

---

## 📋 Project Phases

- [x] **Phase 1** — Repository setup, environment, EDA, data dictionary
- [ ] **Phase 2** — Architecture design document + SME approval
- [ ] **Phase 3** — Infrastructure (Terraform: S3, IAM, Snowflake)
- [ ] **Phase 4** — Ingestion pipeline (Google Drive → S3 Bronze)
- [ ] **Phase 5** — PySpark transformations (Bronze → Silver)
- [ ] **Phase 6** — dbt models (Silver → Gold, metrics, tests)
- [ ] **Phase 7** — Streamlit dashboard (staffing insights + risk flags)
- [ ] **Phase 8** — Airflow orchestration (end-to-end DAG)
- [ ] **Phase 9** — CI/CD, GitHub Actions, final polish

---

## 🔮 v2 Roadmap

- [ ] Clinical outcomes metrics using `NH_QualityMsr_MDS`
- [ ] Inspection score correlation using `NH_SurveySummary`
- [ ] For/nonprofit ownership analysis using `NH_Ownership`
- [ ] Extended quality measures from SNF QRP Provider Data
- [ ] State benchmark comparison using `NH_StateUSAverages`

---

## 📂 Documentation

| Document | Location | Status |
|---|---|---|
| Architecture & Design | `docs/architecture.md` | Phase 2 |
| Data Dictionary | `docs/data_dictionary.md` | Phase 2 |
| Metrics Definitions | `docs/metrics_definition.md` | Phase 2 |
| SME Approval | `docs/sme_approval.md` | Phase 2 |

---

## 📹 Demo

> Loom walkthrough video — added at project completion

---

## 📄 Data Sources

Data sourced from the **Centers for Medicare & Medicaid Services (CMS)**:

- [Payroll Based Journal (PBJ) Daily Nurse Staffing](https://data.cms.gov/quality-of-care/payroll-based-journal-daily-nurse-staffing)
- [Nursing Home Care Data](https://data.cms.gov/provider-data/topics/nursing-homes)
- [SNF Value-Based Purchasing Program](https://data.cms.gov/provider-data/topics/nursing-homes)

---

## 👤 Author

Fulati Paerhati
