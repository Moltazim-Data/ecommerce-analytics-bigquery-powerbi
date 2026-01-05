CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.olist_dwh.dim_seller` AS
SELECT
  seller_id,
  seller_zip_code_prefix,
  seller_city,
  seller_state
FROM `YOUR_PROJECT_ID.olist_stg.sellers`;
SQL

