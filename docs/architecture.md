# Solution Design Document
## Healthcare Staffing Analytics Pipeline

| | |
|---|---|
| **Version** | 1.0 |
| **Date** | March 2026 |
| **Author** | Fulati Paerhati |
| **Status** | SME Approved |
| **Reviewer** | Ninad |

---

## 1. Executive Summary

This document describes the architecture and design decisions for the Healthcare
Staffing Analytics Pipeline — an end-to-end data engineering system that ingests
CMS Payroll-Based Journal (PBJ) nurse staffing data, transforms it through a
medallion lakehouse architecture, and surfaces actionable insights through an
interactive dashboard.

The system processes 1.3 million daily staffing records across 14,564 skilled
nursing facilities in 52 US states and territories, joining four supporting CMS
datasets to produce 10 confirmed metrics covering staffing efficiency, facility
performance, quality outcomes, and operational risk. The primary business
objective is to enable healthcare management to identify understaffed facilities,
track CMS compliance thresholds, and correlate staffing levels with patient
outcomes — all through a single unified view.

---

## 2. Architecture Overview
See the Health Care Project Architecture Design - Dark.drawio.png file

### Pipeline Flow

```
Google Drive (5 CSV source files)
        ↓  Google Drive API — incremental, watermark pattern
Python Extractor → AWS S3 Bronze (raw CSV, partitioned by ingestion_date)
        ↓  PySpark read
Databricks PySpark → AWS S3 Silver (Parquet, partitioned by STATE/WorkDate)
        ↓  Snowflake COPY INTO
Snowflake HEALTHCARE_DB → dbt Core (staging → intermediate → marts)
        ↓  snowflake-connector-python
Streamlit + Plotly Dashboard (3 pages, read-only service account)
```

**Orchestration:** Apache Airflow manages scheduling and dependency resolution
across all layers. Run locally via Docker Compose for development; deployed as
AWS MWAA in production — same DAG code, zero rewrite.

**Infrastructure:** All AWS and Snowflake resources provisioned via Terraform.
No manual console configuration. Infrastructure is version-controlled,
reproducible, and auditable.

---

## 3. Design Decisions

This section documents the "why" behind every major technology choice.
Understanding these decisions is as important as the implementation itself.

### 3.1 Why Snowflake over AWS Redshift?

Snowflake was chosen as the analytics warehouse for four reasons:

**dbt native integration.** dbt was built with Snowflake as its primary target.
The `dbt-snowflake` adapter is the most mature and widely used in the ecosystem.
SQL dialect support, incremental materializations, and test coverage are all
first-class in Snowflake.

**Zero cluster management.** Redshift requires provisioning and managing
clusters, choosing node types, and scaling manually. Snowflake separates compute
from storage — the warehouse auto-scales on demand and costs nothing when idle.
For a project with variable query load, this is the right tradeoff.

**Industry adoption.** Snowflake is the dominant warehouse in analytics
engineering roles. Building this project on Snowflake directly maps to the
skills most in demand in the job market.

**Free trial.** Snowflake's free trial provides sufficient compute and storage
for this project without incurring cost.

---

### 3.2 Why Apache Airflow over AWS Step Functions?

**Portability.** Airflow DAGs are Python code that runs anywhere — locally in
Docker, on a VM, or on AWS MWAA. Step Functions are proprietary to AWS. If the
infrastructure ever moves or the project needs to run locally for development,
Step Functions DAGs cannot be reused. Airflow DAGs travel with the codebase.

**Community and ecosystem.** Airflow is the most widely adopted orchestration
tool in data engineering. The operator library, provider packages, and community
support are unmatched. Every major data platform (Snowflake, Databricks, dbt,
S3) has a maintained Airflow provider.

**Skill transferability.** Airflow experience is directly applicable across
companies and cloud providers. Step Functions knowledge is AWS-specific.

**Development experience.** Running Airflow locally via Docker Compose provides
a full-fidelity development environment before deploying to MWAA. Step Functions
requires AWS connectivity to test.

---

### 3.3 Why PySpark on Databricks over AWS Glue?

**Control.** Glue abstracts Spark in ways that limit control over execution
plans, partitioning strategy, and performance tuning. PySpark on Databricks
exposes the full Spark API — critical for writing production-quality
transformations on 1.3M+ row datasets.

**Delta Lake.** Databricks provides native Delta Lake support — ACID
transactions, time travel, schema evolution, and efficient upserts on top of
S3. Glue does not provide this out of the box.

**Modular code.** Glue encourages notebook-style scripts. This project writes
PySpark as importable Python modules with unit tests — a production pattern that
Glue makes difficult.

**Skill value.** PySpark on Databricks is the most in-demand big data
processing skill in the market, significantly ahead of Glue.

---

### 3.4 Why dbt Core for Transformations?

**Separation of concerns.** Raw transformations (PySpark) and business logic
(dbt SQL) are kept separate. PySpark handles data quality, type casting, and
structural cleaning. dbt handles metric definitions, aggregations, and joins.
Each layer is independently testable and reprocessable.

**Built-in testing.** dbt's test framework enforces data quality at the Gold
layer — `not_null`, `unique`, `accepted_values`, and `relationships` tests run
automatically on every `dbt test` invocation and in CI via GitHub Actions.

**Auto-generated documentation.** `dbt docs generate` produces a searchable
data catalog with full column-level lineage from source CSV to mart table. This
becomes the project's living data dictionary.

**SQL accessibility.** dbt models are plain SQL. Analysts and non-engineers can
read, understand, and contribute to metric definitions without learning Python
or Spark.

---

### 3.5 Why Terraform for Infrastructure as Code?

**Reproducibility.** Every S3 bucket, IAM role, Snowflake database, and
warehouse is defined in `.tf` files and version-controlled in Git. The entire
infrastructure can be destroyed and recreated in minutes with `terraform apply`.
No manual console steps, no configuration drift.

**Auditability.** Every infrastructure change goes through a pull request with
`terraform plan` output in CI. Reviewers see exactly what will change before it
happens.

**Multi-provider support.** Terraform manages both AWS resources (S3, IAM,
Secrets Manager) and Snowflake resources (database, schema, warehouse, roles)
in a single tool. CloudFormation only covers AWS.

**Industry standard.** Terraform is the dominant IaC tool across cloud
providers. This skill transfers directly to any company regardless of their
cloud choice.

---

### 3.6 Why S3 Medallion Architecture (Bronze / Silver / Gold)?

The three-layer medallion pattern is the industry standard for data lake design.
Each layer serves a distinct purpose:

**Bronze — raw, immutable.** Source files land exactly as received, with
metadata columns added (`_ingested_at`, `_source_file`, `_batch_id`). Nothing
is transformed, filtered, or dropped. This layer is the audit trail — if
anything goes wrong downstream, Bronze is the ground truth to reprocess from.

**Silver — cleaned, typed, flagged.** PySpark applies all structural
transformations: casting `WorkDate` from integer to date, standardizing `PROVNUM`
to `ccn`, dropping redundant columns, deriving calculated fields, and flagging
quality issues. Rows are never dropped — anomalies are flagged with boolean
columns (`is_ghost_row`, `is_outlier`) so analysts can investigate.

**Gold — aggregated, metric-ready.** dbt builds mart tables optimized for
dashboard queries. No raw data is exposed to the serving layer — only
pre-aggregated, tested, documented marts.

This pattern ensures each layer is independently reprocessable. If a business
logic rule changes in the Gold layer, only dbt needs to rerun — Bronze and
Silver are untouched.

---

### 3.7 Why Streamlit + Plotly for the Dashboard?

**Python-native.** The entire stack is Python. Streamlit requires no frontend
framework knowledge — dashboard pages are Python scripts. This keeps the
codebase consistent and reduces context switching.

**Snowflake connector.** `snowflake-connector-python` and `snowflake-sqlalchemy`
provide direct, efficient connections from Streamlit to Snowflake marts. Query
results stream directly into Plotly charts.

**Speed of iteration.** Streamlit's live-reload development loop is the fastest
way to build and iterate on data dashboards in Python. A chart change is visible
in seconds.

**Portfolio visibility.** Streamlit apps can be deployed to Streamlit Community
Cloud for free, giving the dashboard a public URL suitable for a portfolio.

AWS QuickSight was considered but rejected — it is vendor-locked to AWS,
requires separate licensing, and does not allow the same level of custom
visualization as Plotly.

---

## 4. Data Flow — Step by Step

The following describes the complete journey of a source file from Google Drive
to the dashboard:

**Step 1 — Trigger.** Airflow DAG runs on a scheduled interval. The first task
checks the watermark table in Snowflake for the last successfully ingested
`_ingested_at` timestamp.

**Step 2 — Extraction.** The Python extractor authenticates to Google Drive via
a service account. It lists files in the source folder, filters to those
modified after the watermark, and downloads them to a local temp directory.

**Step 3 — Bronze landing.** Each file is uploaded to S3 Bronze with three
metadata columns appended: `_ingested_at` (UTC timestamp), `_source_file`
(original filename), `_batch_id` (UUID for this run). The file is stored as-is
in CSV format, partitioned by `ingestion_date`.

**Step 4 — Silver transformation.** Databricks PySpark job reads Bronze,
applies all transformations (type casting, column derivation, quality flagging),
and writes Parquet files to S3 Silver partitioned by `STATE` and `WorkDate`.

**Step 5 — Snowflake load.** A `COPY INTO` command loads Silver Parquet files
into Snowflake raw tables in the `RAW` schema.

**Step 6 — dbt run.** dbt executes staging, intermediate, and mart models in
dependency order. All `dbt test` assertions run after each model.

**Step 7 — Dashboard refresh.** Streamlit queries Snowflake mart tables directly
on page load. Plotly renders charts from query results. No data is cached in
the dashboard layer — every load is a fresh query.

**Step 8 — Watermark update.** On successful completion, Airflow updates the
watermark table with the current run's `_ingested_at` timestamp. The next run
will only process files modified after this point.

---

## 5. Incremental Load Strategy

The pipeline uses a **watermark pattern** for incremental ingestion:

- A `pipeline_metadata` table in Snowflake stores `last_successful_run` per
  source file
- On each Airflow run, the extractor queries this table before calling the
  Google Drive API
- Only files modified after `last_successful_run` are downloaded and processed
- On successful completion, `last_successful_run` is updated atomically

This pattern guarantees **idempotency** — running the pipeline twice with the
same source files produces identical results with no duplicate records.

---

## 6. Data Quality Strategy

Data quality is enforced at two layers:

**Silver layer (PySpark) — flag, never drop.**
All anomalies are flagged with boolean columns rather than filtering rows:

| Flag | Condition | Rows Affected |
|---|---|---|
| `is_ghost_row` | `MDScensus = 0` AND total hours > 0 | 165 rows |
| `is_outlier` | Hours column exceeds IQR upper bound | ~3-5% per column |
| `is_provider_info_missing` | PROVNUM not found in NH_ProviderInfo | 17 providers |

**Gold layer (dbt) — test, never trust.**
Every mart model has dbt tests enforcing:
- `not_null` on all primary keys and critical metric columns
- `unique` on grain columns (e.g. `ccn` + `work_date`)
- `relationships` ensuring foreign keys resolve to dimension tables
- `accepted_values` on categorical columns (e.g. `STATE`)

Tests run automatically in GitHub Actions on every pull request. A failing test
blocks the merge.

---

## 7. Security Design

| Concern | Solution |
|---|---|
| AWS credentials | Stored in `~/.aws/credentials` locally, AWS Secrets Manager in production |
| Snowflake credentials | AWS Secrets Manager, injected at runtime via Airflow connections |
| Google Drive credentials | Service account JSON in AWS Secrets Manager |
| S3 bucket access | Private buckets, IAM roles with least-privilege per service |
| Snowflake access | RBAC — separate roles for pipeline (read/write) and dashboard (read-only) |
| Dashboard credentials | Streamlit `secrets.toml` locally, environment variables in deployment |
| Git repository | `.env` in `.gitignore`, `detect-private-key` pre-commit hook |

No credentials are ever stored in code or committed to Git.

---

## 8. Scalability Design

The architecture is designed to scale beyond Q2 2024 without code changes:

| Concern | Design Decision |
|---|---|
| Data volume | PySpark handles 100× data volume without modification |
| S3 query performance | Partitioned by STATE/WorkDate — query pruning at read time |
| Snowflake compute | Auto-scaling warehouse — no manual intervention |
| New quarters | Incremental load pattern handles new files automatically |
| New metrics | Add a dbt mart model — no pipeline changes required |
| New source files | Add a new extractor task to the Airflow DAG |

---

## 9. AWS Services Used

| Service | Purpose | Justification |
|---|---|---|
| S3 | Data lake — Bronze, Silver, Gold buckets | Cheapest durable object storage at any scale |
| IAM | Access control for all services | Least-privilege roles per service, no shared credentials |
| MWAA | Managed Apache Airflow | No cluster management, same DAG code as local Docker |
| Secrets Manager | Credential storage for all services | Never hardcode secrets, audit trail on access |
| CloudWatch | Pipeline monitoring and alerting | Native AWS logging, Airflow task logs stream here |

---

## 10. Data Model Summary

```
FACT TABLE
└── fact_daily_staffing      1,325,324 rows   PROVNUM × WorkDate

DIMENSION TABLES
├── dim_provider                14,814 rows   NH_ProviderInfo (99.9% match)
├── dim_penalties               28,505 rows   NH_Penalties (62.4% match)
├── dim_quality_claims          59,256 rows   NH_QualityMsr_Claims (99.9% match)
└── dim_vbp_performance         10,858 rows   FY_2024_SNF_VBP (63.9% match)

MART TABLES (dbt Gold layer)
├── mart_staffing_daily         PROVNUM × DATE — daily metrics per facility
├── mart_staffing_by_state      STATE × MONTH — state-level aggregations
├── mart_facility_summary       PROVNUM — Q2 aggregated facility scorecard
├── mart_cms_compliance         Facilities below 3.48 hrs/patient/day threshold
└── mart_penalty_correlation    Staffing level vs penalty amount per facility
```

**Key join standardization:** Master file column `PROVNUM` and all supporting
file column `CMS Certification Number (CCN)` are the same identifier with
different names. Both are standardized to `ccn` (VARCHAR(10)) in the Silver
transformation layer before loading to Snowflake.

---

## 11. Confirmed Calculable Metrics

Based on EDA findings, the following 10 metrics are calculable from available
data. Full definitions and formulas in `docs/metrics_definition.md`.

| # | Metric | Category | Source |
|---|---|---|---|
| 1 | Nurse hours per patient per day | Staffing | Master file |
| 2 | Total hours by hospital, state, month | Staffing | Master file |
| 3 | Bed utilization rate | Facility | Master + NH_ProviderInfo |
| 4 | Staffing levels vs bed occupancy | Facility | Master + NH_ProviderInfo |
| 5 | Top 10 hospitals by patient throughput | Facility | Master file |
| 6 | Facilities with lowest staffing vs load | Facility | Master file |
| 7 | Readmission rates by hospital and state | Quality | NH_QualityMsr_Claims |
| 8 | Staffing vs readmission correlation | Quality | Master + NH_QualityMsr_Claims |
| 9 | Permanent vs contract staff ratio | Operational | Master file |
| 10 | CMS compliance flag (< 3.48 threshold) | Staffing | Master file |

---

## 12. Limitations and Future Work

**Data limitations:**
- Dataset covers Q2 2024 only — occupancy trend analysis is limited to one quarter
- CMS PBJ data is aggregated at facility-day level — individual nurse metrics
  (overtime %, shifts per nurse) are not calculable
- No financial data in public CMS files — all cost metrics are out of scope
- No department-level breakdown — all metrics are at facility level

**v2 enhancements planned:**
- Add `NH_QualityMsr_MDS` for clinical outcome metrics
- Add `NH_SurveySummary` for inspection score correlation
- Add `NH_Ownership` for for/nonprofit ownership analysis
- Extend to Q3/Q4 2024 data as it becomes available
- Add dbt incremental models for quarterly data refresh

---

## 13. Appendix

### A. Repository Structure
See `README.md` for full project structure.

### B. Data Dictionary
See `docs/data_dictionary.md` for full column definitions and EDA findings.

### C. Metrics Definitions
See `docs/metrics_definition.md` for metric formulas, source columns, and
data availability assessment.

### D. SME Approval
See `docs/sme_approval.md` for reviewer sign-off.
# Solution Design Document
## Healthcare Staffing Analytics Pipeline

| | |
|---|---|
| **Version** | 1.1 |
| **Date** | March 2026 |
| **Author** | Your Name |
| **Status** | Pending SME Approval |
| **Reviewer** | Your Instructor / Manager Name |
| **Change from v1.0** | Replaced Snowflake with Databricks Delta Lake per SME feedback |

---

## 1. Executive Summary

This document describes the architecture and design decisions for the Healthcare
Staffing Analytics Pipeline — an end-to-end data engineering system that ingests
CMS Payroll-Based Journal (PBJ) nurse staffing data, transforms it through a
medallion lakehouse architecture, and surfaces actionable insights through an
interactive dashboard.

The system processes 1.3 million daily staffing records across 14,564 skilled
nursing facilities in 52 US states and territories, joining four supporting CMS
datasets to produce 10 confirmed metrics covering staffing efficiency, facility
performance, quality outcomes, and operational risk.

The architecture uses Databricks as the unified compute and storage platform —
handling both heavy PySpark transformations and serving the Gold layer via Delta
Lake and dbt. This simplifies the stack while retaining full testing,
documentation, and lineage capabilities through the dbt-databricks adapter.

---

## 2. Architecture Overview

![Pipeline Architecture](architecture.png)

### Pipeline Flow

```
Google Drive (5 CSV source files)
        ↓  Google Drive API — incremental, watermark pattern
Python Extractor → AWS S3 Bronze (raw CSV, partitioned by ingestion_date)
        ↓  PySpark read
Databricks PySpark → S3 Silver (Delta tables, partitioned by STATE/WorkDate)
        ↓  dbt-databricks
Databricks Delta Lake + dbt → Gold (staging → intermediate → marts)
        ↓  Databricks SQL connector
Streamlit + Plotly Dashboard (3 pages, read-only service principal)
```

**Orchestration:** Apache Airflow manages scheduling and dependency resolution
across all layers. Runs locally via Docker Compose for development, deployed
as AWS MWAA in production — same DAG code, zero rewrite.

**Infrastructure:** All AWS and Databricks resources provisioned via Terraform.
No manual console configuration. Infrastructure is version-controlled,
reproducible, and auditable.

---

## 3. Design Decisions

### 3.1 Why Databricks as the Unified Platform?

Databricks was chosen as both the processing engine and the warehouse layer
for three reasons:

**Unified compute and storage.** Databricks with Delta Lake handles the full
data lifecycle — ingestion, transformation, and serving — on a single platform.
This eliminates the operational overhead of managing a separate warehouse and
reduces the number of data movement steps.

**Delta Lake as the Gold layer.** Delta Lake provides ACID transactions, schema
enforcement, time travel, and efficient upserts on top of S3. This gives the
Gold layer warehouse-grade reliability without requiring a separate service.

**Databricks SQL.** Databricks SQL warehouses provide fast, serverless SQL
execution against Delta tables — suitable for dashboard queries from Streamlit.
The warehouse scales to zero when idle and spins up on demand.

**SME requirement.** The SME review of v1.0 identified Snowflake as an
unnecessary additional service given Databricks already covers the same
capability. This version consolidates to Databricks throughout.

---

### 3.2 Why dbt on Databricks instead of Pure PySpark?

Databricks can run SQL transformations natively. dbt is still used on top
for four reasons that PySpark notebooks do not provide:

**Testing.** dbt's test framework enforces data quality at the Gold layer —
`not_null`, `unique`, `relationships`, and `accepted_values` tests run
automatically on every `dbt test` invocation and in CI via GitHub Actions.
Writing equivalent assertions in PySpark is manual and inconsistent.

**Documentation.** `dbt docs generate` produces a searchable data catalog
with full column-level lineage from source CSV to mart table. This is the
project's living data dictionary and a key deliverable.

**Lineage.** dbt builds a dependency DAG of all models — you can see exactly
which tables depend on which sources. Critical for debugging and impact
analysis when business logic changes.

**Modularity.** dbt's staging → intermediate → mart pattern enforces
separation of concerns in SQL. Without it, transformations accumulate into
large unmanageable notebooks.

---

### 3.3 Why Apache Airflow over AWS Step Functions?

**Portability.** Airflow DAGs are Python code that runs anywhere — locally
in Docker, on a VM, or on AWS MWAA. Step Functions are proprietary to AWS.
Airflow DAGs travel with the codebase and require no cloud connectivity
to develop and test locally.

**Community and ecosystem.** Airflow is the most widely adopted orchestration
tool in data engineering. Every major platform (Databricks, dbt, S3) has a
maintained Airflow provider package.

**Development experience.** Running Airflow locally via Docker Compose
provides a full-fidelity development environment before deploying to MWAA.

**Skill transferability.** Airflow knowledge transfers across companies and
cloud providers. Step Functions knowledge is AWS-specific.

---

### 3.4 Why Terraform for Infrastructure as Code?

**Reproducibility.** Every S3 bucket, IAM role, and Databricks workspace
configuration is defined in `.tf` files and version-controlled in Git.
The entire infrastructure can be destroyed and recreated with `terraform apply`.

**Auditability.** Every infrastructure change goes through a pull request
with `terraform plan` output in CI. Reviewers see exactly what will change
before it happens.

**Multi-provider support.** Terraform manages both AWS resources (S3, IAM,
Secrets Manager) and Databricks resources (workspace, clusters, permissions)
in a single tool.

---

### 3.5 Why S3 Medallion Architecture (Bronze / Silver / Gold)?

**Bronze — raw, immutable.** Source files land exactly as received with
metadata columns added (`_ingested_at`, `_source_file`, `_batch_id`).
Nothing is transformed, filtered, or dropped. This is the audit trail —
if anything goes wrong downstream, Bronze is the ground truth to reprocess from.

**Silver — cleaned, typed, flagged.** PySpark applies all structural
transformations: casting `WorkDate` from integer to date, standardizing
`PROVNUM` to `ccn`, dropping redundant columns, deriving calculated fields,
and flagging quality issues. Written as Delta tables for ACID compliance.
Rows are never dropped — anomalies are flagged with boolean columns.

**Gold — aggregated, metric-ready.** dbt builds mart tables optimized for
dashboard queries. No raw data is exposed to the serving layer — only
pre-aggregated, tested, documented marts.

Each layer is independently reprocessable. If a business logic rule changes
in the Gold layer, only dbt needs to rerun — Bronze and Silver are untouched.

---

### 3.6 Why Streamlit + Plotly for the Dashboard?

**Python-native.** The entire stack is Python. Streamlit requires no
frontend framework knowledge. Dashboard pages are Python scripts that
connect directly to Databricks SQL.

**Databricks connector.** The `databricks-sql-connector` library provides
direct, efficient connections from Streamlit to Databricks SQL warehouses.
Query results stream directly into Plotly charts.

**Speed of iteration.** Streamlit's live-reload development loop is the
fastest way to build and iterate on data dashboards in Python.

**Portfolio visibility.** Streamlit apps can be deployed to Streamlit
Community Cloud for free, giving the dashboard a public URL.

---

## 4. Data Flow — Step by Step

**Step 1 — Trigger.** Airflow DAG runs on scheduled interval. The first
task checks the watermark table in Databricks for the last successfully
ingested `_ingested_at` timestamp.

**Step 2 — Extraction.** The Python extractor authenticates to Google Drive
via a service account. It lists files in the source folder, filters to those
modified after the watermark, and downloads them to a local temp directory.

**Step 3 — Bronze landing.** Each file is uploaded to S3 Bronze with three
metadata columns: `_ingested_at`, `_source_file`, `_batch_id`. Stored as-is
in CSV format, partitioned by `ingestion_date`.

**Step 4 — Silver transformation.** Databricks PySpark job reads Bronze,
applies all transformations, and writes Delta tables to S3 Silver partitioned
by `STATE` and `WorkDate`.

**Step 5 — dbt run.** dbt executes staging, intermediate, and mart models
in dependency order directly on Databricks. All `dbt test` assertions run
after each model.

**Step 6 — Dashboard refresh.** Streamlit queries Databricks SQL warehouse
against Gold mart tables on page load. Plotly renders charts from query
results. No data is cached in the dashboard layer.

**Step 7 — Watermark update.** On successful completion, Airflow updates
the watermark table with the current run's `_ingested_at` timestamp.

---

## 5. Incremental Load Strategy

The pipeline uses a **watermark pattern** for incremental ingestion:

- A `pipeline_metadata` Delta table in Databricks stores `last_successful_run`
  per source file
- On each Airflow run, the extractor queries this table before calling the
  Google Drive API
- Only files modified after `last_successful_run` are downloaded
- On successful completion, `last_successful_run` is updated atomically

This pattern guarantees **idempotency** — running the pipeline twice with
the same source files produces identical results with no duplicate records.

---

## 6. Data Quality Strategy

**Silver layer (PySpark) — flag, never drop.**

| Flag | Condition | Rows Affected |
|---|---|---|
| `is_ghost_row` | `MDScensus = 0` AND total hours > 0 | 165 rows |
| `is_outlier` | Hours column exceeds IQR upper bound | ~3-5% per column |
| `is_provider_info_missing` | PROVNUM not in NH_ProviderInfo | 17 providers |

**Gold layer (dbt) — test, never trust.**
Every mart model has dbt tests enforcing:
- `not_null` on all primary keys and critical metric columns
- `unique` on grain columns
- `relationships` ensuring foreign keys resolve to dimension tables
- `accepted_values` on categorical columns

Tests run automatically in GitHub Actions on every pull request.
A failing test blocks the merge.

---

## 7. Security Design

| Concern | Solution |
|---|---|
| AWS credentials | `~/.aws/credentials` locally, AWS Secrets Manager in production |
| Databricks credentials | AWS Secrets Manager, injected via Airflow connections |
| Google Drive credentials | Service account JSON in AWS Secrets Manager |
| S3 bucket access | Private buckets, IAM roles least-privilege per service |
| Databricks access | Service principals — separate for pipeline vs dashboard |
| Dashboard credentials | Streamlit `secrets.toml` locally, env vars in deployment |
| Git repository | `.env` in `.gitignore`, `detect-private-key` pre-commit hook |

---

## 8. Scalability Design

| Concern | Design Decision |
|---|---|
| Data volume | PySpark handles 100× data volume without code changes |
| S3 query performance | Partitioned by STATE/WorkDate — query pruning at read time |
| Databricks compute | Auto-scaling clusters — no manual intervention |
| New quarters | Incremental load pattern handles new files automatically |
| New metrics | Add a dbt mart model — no pipeline changes required |
| New source files | Add a new extractor task to the Airflow DAG |

---

## 9. AWS Services Used

| Service | Purpose | Justification |
|---|---|---|
| S3 | Data lake — Bronze, Silver, Gold buckets | Cheapest durable object storage at any scale |
| IAM | Access control for all services | Least-privilege roles, no shared credentials |
| MWAA | Managed Apache Airflow | No cluster management, same DAG code as local Docker |
| Secrets Manager | Credential storage | Never hardcode secrets, audit trail on access |
| CloudWatch | Pipeline monitoring | Native AWS logging, Airflow task logs stream here |

---

## 10. Data Model Summary

```
FACT TABLE
└── fact_daily_staffing      1,325,324 rows   PROVNUM × WorkDate

DIMENSION TABLES
├── dim_provider                14,814 rows   NH_ProviderInfo (99.9% match)
├── dim_penalties               28,505 rows   NH_Penalties (62.4% match)
├── dim_quality_claims          59,256 rows   NH_QualityMsr_Claims (99.9% match)
└── dim_vbp_performance         10,858 rows   FY_2024_SNF_VBP (63.9% match)

MART TABLES (dbt Gold layer on Databricks Delta Lake)
├── mart_staffing_daily         PROVNUM × DATE
├── mart_staffing_by_state      STATE × MONTH
├── mart_facility_summary       PROVNUM — Q2 aggregated
├── mart_cms_compliance         Facilities below 3.48 hrs/patient/day
└── mart_penalty_correlation    Staffing level vs penalty amount
```

**Key join standardization:** `PROVNUM` (master file) and
`CMS Certification Number (CCN)` (all supporting files) are the same
identifier. Both standardized to `ccn` (STRING) in the Silver layer.

---

## 11. Confirmed Calculable Metrics

| # | Metric | Category | Source |
|---|---|---|---|
| 1 | Nurse hours per patient per day | Staffing | Master file |
| 2 | Total hours by hospital, state, month | Staffing | Master file |
| 3 | Bed utilization rate | Facility | Master + NH_ProviderInfo |
| 4 | Staffing levels vs bed occupancy | Facility | Master + NH_ProviderInfo |
| 5 | Top 10 hospitals by patient throughput | Facility | Master file |
| 6 | Facilities with lowest staffing vs load | Facility | Master file |
| 7 | Readmission rates by hospital and state | Quality | NH_QualityMsr_Claims |
| 8 | Staffing vs readmission correlation | Quality | Master + NH_QualityMsr_Claims |
| 9 | Permanent vs contract staff ratio | Operational | Master file |
| 10 | CMS compliance flag (< 3.48 threshold) | Staffing | Master file |

---

## 12. Limitations and Future Work

**Data limitations:**
- Dataset covers Q2 2024 only
- No individual nurse records — overtime and shift metrics not calculable
- No financial data — all cost metrics out of scope
- No department-level breakdown — all metrics at facility level

**v2 enhancements:**
- Add `NH_QualityMsr_MDS` for clinical outcome metrics
- Add `NH_SurveySummary` for inspection score correlation
- Extend to Q3/Q4 2024 as data becomes available
- Add dbt incremental models for quarterly refresh

---

## 13. Change Log

| Version | Date | Author | Change |
|---|---|---|---|
| 1.0 | March 2026 | Your Name | Initial submission |
| 1.1 | March 2026 | Your Name | Replaced Snowflake with Databricks Delta Lake per SME feedback |

---

## 14. Appendix

### A. Repository Structure
See `README.md` for full project structure.

### B. Data Dictionary
See `docs/data_dictionary.md` for full column definitions and EDA findings.

### C. Metrics Definitions
See `docs/metrics_definition.md` for metric formulas and data availability.

### D. SME Approval
See `docs/sme_approval.md` for reviewer sign-off.
