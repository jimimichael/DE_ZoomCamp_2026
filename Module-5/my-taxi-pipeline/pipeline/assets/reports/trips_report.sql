/* @bruin

# Docs:
# - SQL assets: https://getbruin.com/docs/bruin/assets/sql
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks: https://getbruin.com/docs/bruin/quality/available_checks

# Asset metadata
name: reports.trips_report
type: duckdb.sql
depends:
  - staging.trips

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: date

columns:
  - name: taxi_type
    type: TEXT
    description: Taxi type (e.g., yellow, green)
    primary_key: true
  - name: pickup_date
    type: DATE
    description: Pickup date
    primary_key: true
  - name: trip_count
    type: BIGINT
    description: Number of trips
    checks:
      - name: non_negative

@bruin */

-- Purpose of reports:
-- - Aggregate staging data for dashboards and analytics
-- Required Bruin concepts:
-- - Filter using `{{ start_datetime }}` / `{{ end_datetime }}` for incremental runs
-- - GROUP BY your dimension + date columns

SELECT 
  pickup_datetime::DATE AS pickup_date,
  taxi_type,
  COUNT(*) AS trip_count
FROM staging.trips
WHERE pickup_datetime >= '{{ start_datetime }}'
  AND pickup_datetime < '{{ end_datetime }}'
GROUP BY pickup_date, taxi_type
ORDER BY pickup_date, taxi_type