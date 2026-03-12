# 🏥 Healthcare Staffing Analytics Pipeline

> An end-to-end, production-grade data pipeline and interactive analytics dashboard
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

This project ingests CMS Payroll-Based Journal (PBJ) staffing data alongside
15 supporting datasets to build a scalable analytics platform that surfaces
these insights through an interactive dashboard.

---

## 🏗️ Architecture

> Architecture diagram will be added after Excalidraw design — Phase 2

**Pipeline Flow:**

```
Google Drive (Source)
      ↓
AWS S3 — Bronze Layer (Raw, immutable)
      ↓
PySpark on Databricks — Silver Layer (Cleaned, typed, deduplicated)
      ↓
Snowflake + dbt — Gold Layer (Metrics, aggregations, marts)
      ↓
Streamlit + Plotly — Serving Layer (Interactive dashboard)
```

**Orchestration:** Apache Airflow (Docker locally → MWAA in production)
**Infrastructure:** Terraform (all AWS + Snowflake resources provisioned as code)
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
| Code Quality | black + flake8 + sqlfluff | Consistent formatting enforced via pre-commit |

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
├── docs/                             # Architecture and design documents
│   ├── architecture.md
│   ├── data_dictionary.md
│   └── sme_approval.md
│
├── .env.example                      # Environment variable template
├── .gitignore
├── .pre-commit-config.yaml
├── .python-version                   # Pins Python 3.11.9 via pyenv
├── Makefile
├── pyproject.toml                    # Poetry dependency management
├── poetry.lock                       # Locked dependency versions
└── README.md
```

---

## 📊 Source Data

### Master Dataset
| File | Description | Rows (approx) |
|---|---|---|
| `PBJ_Daily_Nurse_Staffing_Q2_2024.csv` | Daily nurse staffing hours by provider — Q2 2024 | TBD after EDA |

### Supporting Datasets (15 files)
| File | Description |
|---|---|
| `NH_ProviderInfo_Oct2024.csv` | Provider details — name, address, facility type |
| `NH_Ownership_Oct2024.csv` | Facility ownership information |
| `NH_Penalties_Oct2024.csv` | Fines and penalties per facility |
| `NH_QualityMsr_Claims_Oct2024.csv` | Quality measures derived from claims |
| `NH_QualityMsr_MDS_Oct2024.csv` | Quality measures from MDS assessments |
| `NH_StateUSAverages_Oct2024.csv` | State-level benchmark averages |
| `NH_SurveyDates_Oct2024.csv` | Facility inspection dates |
| `NH_SurveySummary_Oct2024.csv` | Inspection summary results |
| `NH_HealthCitations_Oct2024.csv` | Health violation citations |
| `NH_FireSafetyCitations_Oct2024.csv` | Fire safety violation citations |
| `NH_CitationDescriptions_Oct2024.csv` | Citation code descriptions |
| `NH_CovidVaxAverages_Oct2024.csv` | COVID vaccination rate averages |
| `NH_CovidVaxProvider_Oct2024.csv` | COVID vaccination data per provider |
| `FY_2024_SNF_VBP_Facility_Performance.csv` | Value-based purchasing — facility level |
| `FY_2024_SNF_VBP_Aggregate_Performance.csv` | Value-based purchasing — aggregate |

---

## 📋 Key Metrics
> To be finalized after EDA — Phase 1

Planned metrics include:
- Nurse-to-patient ratio by facility, state, and month
- Contract vs. employed staff ratio (operational risk indicator)
- Total staffing hours by role and region
- Staffing intensity score (hours per census, normalized)
- Zero-staffing day flags (census > 0 but near-zero hours)
- Correlation between staffing levels and quality/penalty outcomes

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
git clone https://github.com/fulati-paerhat-john/healthcare-staffing-pipeline
cd healthcare-staffing-pipeline

# Activate correct Python version (pyenv reads .python-version automatically)
pyenv local 3.11.9

# Install dependencies
poetry install

# Activate virtual environment
eval $(poetry env activate)

# Set up pre-commit hooks
pre-commit install

# Copy environment variables template
cp .env.example .env
# Fill in your credentials in .env
```

### AWS Configuration
```bash
aws configure --profile healthcare-pipeline
# Enter your IAM user Access Key ID and Secret Access Key
# Region: us-east-1

# Verify
aws sts get-caller-identity --profile healthcare-pipeline
```

---

## 📋 Project Phases

- [x] **Phase 1** — Repository setup, environment, EDA, data dictionary
- [ ] **Phase 2** — Infrastructure (Terraform: S3, IAM, Snowflake)
- [ ] **Phase 3** — Ingestion pipeline (Google Drive → S3 Bronze)
- [ ] **Phase 4** — PySpark transformations (Bronze → Silver)
- [ ] **Phase 5** — dbt models (Silver → Gold, metrics, tests)
- [ ] **Phase 6** — Streamlit dashboard (staffing insights + risk flags)
- [ ] **Phase 7** — Airflow orchestration (end-to-end DAG)
- [ ] **Phase 8** — CI/CD, GitHub Actions, final polish

---

## 📂 Data Dictionary
> See [`docs/data_dictionary.md`](docs/data_dictionary.md) — updated after EDA

---

## 🏛️ Architecture Decision Record
> See [`docs/architecture.md`](docs/architecture.md)

---

## 📹 Demo
> Loom walkthrough video — added at project completion

---

## 🤝 Contributing
This is a portfolio project. Feedback and suggestions are welcome via GitHub Issues.

---

## 👤 Author
Fulati Paerhati
[LinkedIn](#) · [GitHub](#) · [Portfolio](#)

---

## 📄 Data Source
Data sourced from the **Centers for Medicare & Medicaid Services (CMS)**
- [Payroll Based Journal (PBJ) Daily Nurse Staffing](https://data.cms.gov/quality-of-care/payroll-based-journal-daily-nurse-staffing)
- [Nursing Home Care Data](https://data.cms.gov/provider-data/topics/nursing-homes)
