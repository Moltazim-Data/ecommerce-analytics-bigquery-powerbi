CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.olist_stg.customers` AS
SELECT
  customer_id,
  customer_unique_id,
  SAFE_CAST(customer_zip_code_prefix AS STRING) AS customer_zip_code_prefix,
  customer_city,
  customer_state
FROM `YOUR_PROJECT_ID.olist_raw.customers`
WHERE customer_id IS NOT NULL;
SQL

