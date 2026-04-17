# Metadata Dictionary

This file documents the key tables and fields used in the UK Financial Complaints Pipeline.

## Bronze Layer

### `bronze_complaints.complaints_aggregate`
| Column | Type | Description |
|---|---|---|
| `semester` | STRING | Semiannual reporting period, e.g. `2025_H1` |
| `product_group` | STRING | FCA complaint product grouping or category |
| `variable_type` | STRING | A type of metric or complaint classification |
| `variable` | STRING | Specific complaint variable or description |
| `metric_value` | NUMERIC | Raw source volume metric from FCA data; may represent complaints, redress, provision, or other metrics |
| `data_type` | STRING | Source classification, e.g. `aggregate` or `firm_specific` |
| `reporting_period` | STRING | Fixed reporting label added during ingestion |

### `bronze_complaints.complaints_firm_specific`
| Column | Type | Description |
|---|---|---|
| `firm_name` | STRING | Name of the reported financial firm |
| `uphold_rate` | NUMERIC | Share of complaints upheld by the firm |
| `total_complaints` | NUMERIC | Total complaints recorded for the firm |
| `data_type` | STRING | Source classification, expected to be `firm_specific` |
| `semester` | STRING | Semiannual reporting period |
| `reporting_period` | STRING | Fixed reporting label added during ingestion |

> Note: the firm-specific staging model currently uses a placeholder query because the raw source schema requires alignment before full firm-level transformation is active.

## Silver Layer

### `silver_complaints.int_complaints_aggregate_cleaned`
| Column | Type | Description |
|---|---|---|
| `semester` | STRING | Semiannual reporting period |
| `product_group` | STRING | Original FCA product grouping |
| `variable_type` | STRING | Complaint metric type |
| `variable` | STRING | Complaint variable name or dimension |
| `metric_value` | NUMERIC | Raw source metric value loaded from bronze |
| `complaint_count` | NUMERIC | Complaint totals only for complaint metrics; excludes redress and provision rows |
| `redress_paid` | NUMERIC | Redress totals where `variable_type` = `Redress paid` |
| `provision_amount` | NUMERIC | Provision totals where `variable_type` = `Provision` |
| `data_type` | STRING | Source classification |
| `reporting_period` | STRING | Reporting label added in bronze layer |
| `product_category_clean` | STRING | Standardized product category group (Banking, Credit, Insurance, Investments, Other) |
| `complaint_volume_category` | STRING | Volume bucket (High, Medium, Low) based on complaint_count for complaint metrics |

### `silver_complaints.int_complaints_firm_cleaned`
| Column | Type | Description |
|---|---|---|
| `firm_name` | STRING | Raw firm name from source |
| `uphold_rate` | NUMERIC | Firm uphold rate |
| `total_complaints` | NUMERIC | Firm complaint count |
| `data_type` | STRING | Source classification |
| `semester` | STRING | Semiannual reporting period |
| `reporting_period` | STRING | Reporting label added in bronze layer |
| `firm_name_clean` | STRING | Uppercase, trimmed firm identifier |
| `uphold_rate_category` | STRING | Uphold rate band (High, Medium, Low) |

## Gold Layer

### `gold_complaints.dim_product`
| Column | Type | Description |
|---|---|---|
| `product_id` | STRING | Product identifier, sourced from `product_group` |
| `product_name` | STRING | Product display name |
| `product_category` | STRING | Standardized product category |

### `gold_complaints.fct_complaints_monthly`
| Column | Type | Description |
|---|---|---|
| `complaint_period` | STRING | Semiannual period label matching `semester` |
| `product_group` | STRING | Original FCA product group |
| `metric_type` | STRING | Complaint metric type |
| `total_complaints` | NUMERIC | Sum of complaint totals for the grouping |
| `total_redress_paid` | NUMERIC | Sum of redress amounts for the grouping |
| `total_provision_amount` | NUMERIC | Sum of provision amounts for the grouping |
| `total_metric_value` | NUMERIC | Sum of raw source metric values for the grouping |
| `number_of_records` | INT64 | Number of rows aggregated into the fact record |
| `avg_complaints_per_record` | NUMERIC | Average complaint count per row |
| `product_category` | STRING | Standardized product category |
| `complaint_volume_category` | STRING | Volume bucket classification |

#### Dashboard query example
```sql
SELECT
  complaint_period,
  product_group,
  metric_type,
  total_complaints,
  total_redress_paid,
  total_provision_amount,
  total_metric_value
FROM `uk-complaints-analytics.gold_complaints.fct_complaints_monthly`
WHERE metric_type IN ('Complaints received', 'Redress paid', 'Provision')
ORDER BY complaint_period DESC, product_group, metric_type;
```
This query makes the redress and provision metrics explicit for dashboard widgets while preserving complaint totals separately.

### `gold_complaints.fct_complaints_semiannual`
| Column | Type | Description |
|---|---|---|
| `semester` | STRING | Semiannual reporting period |
| `year` | DATE | Year parsed from `semester` |
| `half` | STRING | First Half / Second Half indicator |
| `period_end_date` | DATE | End-of-period date for the semester |
| `product_group` | STRING | Original FCA product group |
| `variable_type` | STRING | Complaint metric type |
| `variable` | STRING | Complaint variable name |
| `metric_value` | NUMERIC | Raw source metric value from silver; includes complaint, redress, and provision volumes |
| `complaint_count` | NUMERIC | Complaint volume for the row; only populated for complaint metrics after silver-layer cleansing |
| `data_type` | STRING | Source classification |
| `data_source` | STRING | Hard-coded source label `FCA` |

### `gold_complaints.dim_firm`
| Column | Type | Description |
|---|---|---|
| `firm_id` | STRING | Cleaned firm identifier |
| `firm_name` | STRING | Cleaned firm name |
| `uphold_rate_category` | STRING | Firm-level uphold rate category |
| `avg_uphold_rate` | NUMERIC | Average uphold rate across records |
| `total_complaints_all_time` | NUMERIC | Total complaints aggregated for the firm |
| `complaint_records_count` | INT64 | Number of complaint records for the firm |

## Source File Mapping
- `fca_aggregate_2025_H1.xlsx` → `Product Group` sheet → `complaints_aggregate`
- `fca_firm_specific_2025_H1.xlsx` → `Opened` sheet → `complaints_firm_specific`

## Notes
- The pipeline uses the `europe-west2` region for all GCP resources.
- Raw files are uploaded to GCS before BigQuery ingestion.
- The metadata dictionary is intended to document the current model names and field transformations used in the project.
