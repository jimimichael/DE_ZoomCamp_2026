{{ config(
  materialized='table',
  partition_by={'field': 'complaint_period', 'data_type': 'string'},
  cluster_by=['product_group', 'metric_type']
) }}

SELECT
  semester as complaint_period,  -- Note: data is semiannual, not monthly
  product_group,
  variable_type as metric_type,
  SUM(COALESCE(complaint_count, 0)) as total_complaints,
  SUM(COALESCE(redress_paid, 0)) as total_redress_paid,
  SUM(COALESCE(provision_amount, 0)) as total_provision_amount,
  SUM(COALESCE(metric_value, 0)) as total_metric_value,
  COUNT(*) as number_of_records,
  AVG(complaint_count) as avg_complaints_per_record,
  product_category_clean as product_category,
  complaint_volume_category
FROM {{ ref('int_complaints_aggregate_cleaned') }}
GROUP BY
  semester,
  product_group,
  variable_type,
  product_category_clean,
  complaint_volume_category