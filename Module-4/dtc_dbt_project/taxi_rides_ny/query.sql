SELECT 'fct_monthly_zone_revenue' as table_name, COUNT(*) as row_count FROM dbt_jimimichael_prod.fct_monthly_zone_revenue
UNION ALL
SELECT 'fct_trips', COUNT(*) FROM dbt_jimimichael_prod.fct_trips
UNION ALL
SELECT 'dim_zones', COUNT(*) FROM dbt_jimimichael_prod.dim_zones
UNION ALL
SELECT 'int_trips', COUNT(*) FROM dbt_jimimichael_prod.int_trips;
