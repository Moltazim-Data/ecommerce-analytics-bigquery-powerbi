CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.olist_stg.order_items` AS
SELECT
  order_id,
  SAFE_CAST(order_item_id AS INT64) AS order_item_id,
  product_id,
  seller_id,
  SAFE_CAST(shipping_limit_date AS TIMESTAMP) AS shipping_limit_ts,
  SAFE_CAST(price AS NUMERIC) AS price,
  SAFE_CAST(freight_value AS NUMERIC) AS freight_value
FROM `YOUR_PROJECT_ID.olist_raw.order_items`
WHERE order_id IS NOT NULL;
SQL

