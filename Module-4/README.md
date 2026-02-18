# Module 4: Analytics Engineering - Homework

Answers:


Question 3.
SELECT 
Count(*) FROM dbt_jimimichael_prod.fct_monthly_zone_revenue;

cd /workspaces/DE_ZoomCamp_2026/Module-4/dtc_dbt_project/taxi_rides_ny
duckdb taxi_data.duckdb

SELECT COUNT(*) FROM dbt_jimimichael_prod.fct_monthly_zone_revenue;




duckdb /workspaces/DE_ZoomCamp_2026/Module-4/dtc_dbt_project/taxi_rides_ny/taxi_data.duckdb -c "SELECT COUNT(*) FROM dbt_jimimichael_prod.fct_monthly_zone_revenue;"


Question 4.
SELECT
    pickup_zone,
    SUM(revenue_monthly_total_amount) as total_revenue
FROM dbt_jimimichael_prod.fct_monthly_zone_revenue
WHERE service_type = 'Green' AND EXTRACT(YEAR FROM revenue_month) = 2020
GROUP BY pickup_zone
ORDER BY total_revenue DESC
LIMIT 1;

Question 5.
SELECT
    SUM(total_monthly_trips) as total_trips
FROM dbt_jimimichael_prod.fct_monthly_zone_revenue
WHERE service_type = 'Green'
  AND EXTRACT(YEAR FROM revenue_month) = 2019
  AND EXTRACT(MONTH FROM revenue_month) = 10;

Question 6.
cd /workspaces/DE_ZoomCamp_2026/Module-4/dtc_dbt_project/taxi_rides_ny && duckdb taxi_data.duckdb 
"SELECT COUNT(*) as fhv_records, COUNT(DISTINCT dispatching_base_num) as unique_bases FROM dbt_jimimichael_prod.stg_fhv_tripdata; 

SELECT COUNT(*) FROM dbt_jimimichael_prod.stg_fhv_tripdata WHERE pickup_datetime < '2019-02-01';"


</details>
