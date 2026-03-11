# Taxi pipeline homework

## Pipeline

- **Pipeline name**: `taxi_pipeline`
- **Source**: `https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api`
- **Destination**: DuckDB (`taxi_pipeline.duckdb`)

## Answers

- **Question 1 – date range**: 2009‑06‑01 to 2009‑07‑01
- **Question 2 – proportion credit card**: 26.66%
- **Question 3 – total tip amount**: $6,063.41

## How to run

python taxi_pipeline.py
python -m dlt pipeline taxi_pipeline show
python -m marimo edit taxi_dashboard.py
