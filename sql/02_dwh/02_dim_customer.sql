CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.olist_dwh.dim_customer` AS
SELECT
  customer_id,
  customer_unique_id,
  customer_zip_code_prefix,
  customer_city,
  customer_state
FROM `YOUR_PROJECT_ID.olist_stg.customers`;
SQL

