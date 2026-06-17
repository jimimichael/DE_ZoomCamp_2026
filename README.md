# Data Engineering ZoomCamp 2026 — Cohort Portfolio

> **A structured learning portfolio covering the full modern data engineering stack: containerisation, infrastructure-as-code, cloud data warehousing, workflow orchestration, data transformation, and stream processing — completed as part of the DataTalks.Club Data Engineering ZoomCamp 2026 cohort.**

[![Python](https://img.shields.io/badge/Python-3.9+-blue)](https://python.org)
[![dbt](https://img.shields.io/badge/dbt-BigQuery-orange)](https://getdbt.com)
[![Terraform](https://img.shields.io/badge/IaC-Terraform-purple)](https://terraform.io)
[![GCP](https://img.shields.io/badge/Cloud-GCP-BigQuery-yellow)](https://cloud.google.com)
[![Docker](https://img.shields.io/badge/Container-Docker-blue)](https://docker.com)
[![Kestra](https://img.shields.io/badge/Orchestration-Kestra-green)](https://kestra.io)

---

## What This Repository Contains

This is the coursework and project portfolio for the [DataTalks.Club Data Engineering ZoomCamp 2026](https://github.com/DataTalks-Club/data-engineering-zoomcamp). It covers each module of the curriculum through hands-on implementation, culminating in a capstone project — the [UK Financial Complaints Analytics Platform](https://github.com/jimimichael/uk-financial-complaints-pipeline).

---

## Curriculum Modules Covered

### Module 1 — Containerisation & Infrastructure as Code
**Tools:** Docker, Docker Compose, Terraform, Google Cloud Platform

Key skills developed:
- Containerising data pipelines with Docker
- Managing multi-service environments with Docker Compose
- Provisioning cloud infrastructure (GCS buckets, BigQuery datasets) with Terraform
- Understanding IaC principles for reproducible, version-controlled infrastructure

```bash
# Example: spin up local Postgres + pgAdmin environment
docker-compose up -d
```

---

### Module 2 — Workflow Orchestration
**Tools:** Kestra

Key skills developed:
- Designing DAG-equivalent workflows in Kestra
- Scheduling and triggering batch pipeline runs
- Chaining ingestion → transformation → testing tasks
- Managing workflow dependencies and failure handling

---

### Module 3 — Data Warehouse
**Tools:** Google BigQuery

Key skills developed:
- BigQuery architecture: datasets, tables, partitioning, clustering
- Writing optimised SQL for analytical queries (window functions, CTEs)
- Understanding query cost and performance optimisation
- Designing for the medallion architecture (Bronze / Silver / Gold)

---

### Module 4 — Analytics Engineering
**Tools:** dbt (data build tool), BigQuery

Key skills developed:
- Building modular, reusable SQL transformation models
- Writing dbt tests for data quality assurance
- Generating data documentation from model metadata
- Implementing dimensional modelling (star schema — fact and dimension tables)
- Using Jinja templating for DRY transformation logic

**Data model design principle applied:**
```
Bronze (raw) → Silver (cleaned + validated) → Gold (analytics-ready)
```

Star schema example from capstone:
```
dim_product ──┐
dim_firm ─────┼──→ fct_complaints_monthly
dim_date ─────┘
```

---

### Module 5 — Batch Processing
**Tools:** Python, Pandas, dlt (Data Load Tool)

Key skills developed:
- Building batch ELT pipelines from public data sources
- Handling raw file formats (Excel, CSV) at scale
- Loading data into cloud storage and data warehouses
- Applying validation checks at ingestion stage

---

### Module 6 — Stream Processing
**Tools:** Kafka (concepts and implementation)

Key skills developed:
- Understanding event streaming architecture
- Producing and consuming messages with Kafka
- Comparing batch vs. streaming use cases

---

## Capstone Project

The coursework in this repository feeds directly into the capstone:

**[UK Financial Complaints Analytics Platform](https://github.com/jimimichael/uk-financial-complaints-pipeline)**

A production-grade end-to-end pipeline that:
- Ingests public FCA complaint data via dlt → Google Cloud Storage → BigQuery
- Transforms through Bronze / Silver / Gold layers using dbt
- Orchestrates monthly batch runs with Kestra
- Provisions infrastructure with Terraform
- Surfaces compliance analytics in a Looker Studio dashboard
- Validates with GitHub Actions CI/CD

---

## Full Tech Stack Covered

| Area | Tools |
|---|---|
| Containerisation | Docker, Docker Compose |
| Infrastructure as Code | Terraform |
| Cloud Platform | Google Cloud Platform (GCS, BigQuery) |
| Orchestration | Kestra (Airflow-equivalent DAG workflows) |
| Data Ingestion | dlt (Data Load Tool), Python |
| Data Warehouse | Google BigQuery |
| Transformation | dbt (models, tests, documentation, Jinja) |
| Data Modelling | Star schema — fact / dimension tables |
| Stream Processing | Apache Kafka |
| Programming | Python (Pandas, NumPy) |
| SQL | Advanced SQL — window functions, CTEs, query optimisation |
| CI/CD | GitHub Actions |
| Visualisation | Looker Studio |

---

## Why This Programme

The Data Engineering ZoomCamp is an intensive, project-based curriculum that covers the tooling used in production data engineering roles. Unlike certification courses, it requires building and running real pipelines — not just watching videos. The capstone project produces a deployable end-to-end system using the same stack found in professional data engineering teams.

---

## Repository Structure

```
DE_ZoomCamp_2026/
├── module_1_docker_terraform/    # Containerisation + IaC exercises
├── module_2_kestra/              # Orchestration workflows
├── module_3_bigquery/            # Data warehouse SQL + optimisation
├── module_4_dbt/                 # Analytics engineering models + tests
├── module_5_batch/               # Batch pipeline exercises
├── module_6_kafka/               # Stream processing exercises
├── homework/                     # Weekly homework submissions
└── README.md
```

---

## Completion

**Status:** ✅ Completed — May 2026

**Completion Certificate:** Available on request

**Capstone:** [UK Financial Complaints Analytics Platform](https://github.com/jimimichael/uk-financial-complaints-pipeline)

---

## Skills Demonstrated

`Docker` `Terraform` `Google Cloud Platform` `BigQuery` `dbt` `Kestra` `dlt` `Apache Kafka` `Python` `Pandas` `SQL` `Star Schema` `Medallion Architecture` `Data Modelling` `CI/CD` `GitHub Actions` `Looker Studio` `Data Engineering`

---

*Part of a portfolio demonstrating applied data engineering and analytics. See also: [UK Financial Complaints Pipeline](https://github.com/jimimichael/uk-financial-complaints-pipeline) | [Sentinel Tax Risk Engine](https://github.com/jimimichael/sentinel-tax-risk-engine) | [APHA Animal Health Dashboard](https://github.com/jimimichael/APHA-Animal-Health-Welfare-Outbreak-Dashboard)*
