CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.olist_stg.orders` AS
SELECT
  order_id,
  customer_id,
  order_status,
  SAFE_CAST(order_purchase_timestamp AS TIMESTAMP) AS order_purchase_ts,
  SAFE_CAST(order_approved_at AS TIMESTAMP) AS order_approved_ts,
  SAFE_CAST(order_delivered_carrier_date AS TIMESTAMP) AS delivered_carrier_ts,
  SAFE_CAST(order_delivered_customer_date AS TIMESTAMP) AS delivered_customer_ts,
  SAFE_CAST(order_estimated_delivery_date AS TIMESTAMP) AS estimated_delivery_ts
FROM `YOUR_PROJECT_ID.olist_raw.orders`
WHERE order_id IS NOT NULL;
SQL

