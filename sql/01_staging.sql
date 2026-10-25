-- BigQuery Standard SQL
-- Replace YOUR_PROJECT_ID and load the named Olist CSV tables into olist_raw first.
CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.olist_stg.orders` AS
SELECT
  order_id,
  customer_id,
  LOWER(TRIM(order_status)) AS order_status,
  SAFE_CAST(order_purchase_timestamp AS TIMESTAMP) AS order_purchase_ts,
  SAFE_CAST(order_approved_at AS TIMESTAMP) AS order_approved_ts,
  SAFE_CAST(order_delivered_carrier_date AS TIMESTAMP) AS order_delivered_carrier_ts,
  SAFE_CAST(order_delivered_customer_date AS TIMESTAMP) AS order_delivered_customer_ts,
  SAFE_CAST(order_estimated_delivery_date AS TIMESTAMP) AS order_estimated_delivery_ts
FROM `YOUR_PROJECT_ID.olist_raw.orders`
WHERE order_id IS NOT NULL;

CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.olist_stg.order_items` AS
SELECT
  order_id,
  SAFE_CAST(order_item_id AS INT64) AS order_item_id,
  product_id,
  seller_id,
  SAFE_CAST(shipping_limit_date AS TIMESTAMP) AS shipping_limit_ts,
  SAFE_CAST(price AS NUMERIC) AS item_price,
  SAFE_CAST(freight_value AS NUMERIC) AS freight_value
FROM `YOUR_PROJECT_ID.olist_raw.order_items`
WHERE order_id IS NOT NULL;

CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.olist_stg.customers` AS
SELECT
  customer_id,
  customer_unique_id,
  TRIM(customer_city) AS customer_city,
  UPPER(TRIM(customer_state)) AS customer_state
FROM `YOUR_PROJECT_ID.olist_raw.customers`
WHERE customer_id IS NOT NULL;

CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.olist_stg.products` AS
SELECT
  product_id,
  product_category_name
FROM `YOUR_PROJECT_ID.olist_raw.products`
WHERE product_id IS NOT NULL;
