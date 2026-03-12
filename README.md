# 🏥 Healthcare Staffing Analytics Pipeline

> End-to-end data pipeline and analytics dashboard
> for hospital nurse staffing and operational metrics
> across US healthcare facilities (Q2 2024).

---

## 📌 Problem Statement
Healthcare networks struggle to get a unified view of
staffing efficiency, nurse-to-patient ratios, and
operational performance across facilities. This project
builds a production-grade data pipeline to surface those
insights through an interactive dashboard.

---

## 🏗️ Architecture
<!-- Add Excalidraw diagram here after design phase -->

**Pipeline Flow:**
Google Drive → S3 (Bronze) → Databricks/PySpark (Silver)
→ Snowflake + dbt (Gold) → Streamlit Dashboard

---

## 🛠️ Tech Stack

| Layer | Tool | Purpose |
|---|---|---|
| Ingestion | Python + Google Drive API | Extract CSVs to S3 |
| Storage | AWS S3 | Data lake (Bronze/Silver/Gold) |
| Processing | PySpark + Databricks | Large-scale transformation |
| Warehouse | Snowflake | Analytics warehouse |
| Modeling | dbt Core | Metrics, testing, lineage |
| Orchestration | Apache Airflow (Docker) | Pipeline scheduling |
| IaC | Terraform | AWS + Snowflake provisioning |
| Dashboard | Streamlit + Plotly | Interactive visualization |
| CI/CD | GitHub Actions | dbt tests + Terraform plan |

---

## 📁 Project Structure
\`\`\`
healthcare-staffing-pipeline/
├── infra/
├── ingestion/
├── spark_jobs/
├── dbt/
├── airflow/
├── dashboard/
├── notebooks/
├── tests/
└── docs/
\`\`\`

---

## 🚀 Getting Started
### Prerequisites
<!-- Fill as you build each phase -->

### Installation
<!-- Fill as you build each phase -->

---

## 📊 Key Metrics
<!-- Fill after EDA and dbt modeling -->

---

## 📋 Project Phases
- [x] Phase 1: Repo setup + EDA
- [ ] Phase 2: Infrastructure (Terraform + S3)
- [ ] Phase 3: Ingestion pipeline (Bronze)
- [ ] Phase 4: PySpark transformations (Silver)
- [ ] Phase 5: dbt models (Gold)
- [ ] Phase 6: Streamlit dashboard
- [ ] Phase 7: Airflow orchestration
- [ ] Phase 8: CI/CD + final polish

---

## 📹 Demo
<!-- Add Loom video link at end of project -->

---

## 👤 Author
Fulati Paerhati
