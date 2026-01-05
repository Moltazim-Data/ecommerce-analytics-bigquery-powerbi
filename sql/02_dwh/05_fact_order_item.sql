CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.olist_dwh.fact_order_item` AS
SELECT
  oi.order_id,
  oi.order_item_id,
  o.customer_id,
  oi.product_id,
  oi.seller_id,
  DATE(o.order_purchase_ts) AS purchase_date,
  o.order_status,
  oi.shipping_limit_ts,
  oi.price,
  oi.freight_value,
  (oi.price + oi.freight_value) AS gross_item_value
FROM `YOUR_PROJECT_ID.olist_stg.order_items` oi
JOIN `YOUR_PROJECT_ID.olist_stg.orders` o
  ON oi.order_id = o.order_id;
SQL

