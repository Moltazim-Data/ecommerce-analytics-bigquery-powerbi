CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.olist_stg.sellers` AS
SELECT
  seller_id,
  SAFE_CAST(seller_zip_code_prefix AS STRING) AS seller_zip_code_prefix,
  seller_city,
  seller_state
FROM `YOUR_PROJECT_ID.olist_raw.sellers`
WHERE seller_id IS NOT NULL;
SQL

