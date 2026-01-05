CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.olist_stg.order_payments` AS
SELECT
  order_id,
  SAFE_CAST(payment_sequential AS INT64) AS payment_sequential,
  payment_type,
  SAFE_CAST(payment_installments AS INT64) AS payment_installments,
  SAFE_CAST(payment_value AS NUMERIC) AS payment_value
FROM `YOUR_PROJECT_ID.olist_raw.order_payments`
WHERE order_id IS NOT NULL;
SQL

