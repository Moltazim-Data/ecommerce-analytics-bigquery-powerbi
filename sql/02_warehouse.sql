-- BigQuery Standard SQL
-- This is a starter dimensional model; validate source uniqueness and row grain after loading.
CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.olist_dwh.dim_customers` AS
SELECT
  customer_id,
  ANY_VALUE(customer_unique_id) AS customer_unique_id,
  ANY_VALUE(customer_city) AS customer_city,
  ANY_VALUE(customer_state) AS customer_state
FROM `YOUR_PROJECT_ID.olist_stg.customers`
GROUP BY customer_id;

CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.olist_dwh.dim_products` AS
SELECT
  product_id,
  ANY_VALUE(product_category_name) AS product_category_name
FROM `YOUR_PROJECT_ID.olist_stg.products`
GROUP BY product_id;

CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.olist_dwh.fact_order_items` AS
SELECT
  i.order_id,
  i.order_item_id,
  o.customer_id,
  i.product_id,
  i.seller_id,
  o.order_status,
  DATE(o.order_purchase_ts) AS order_date,
  i.item_price,
  i.freight_value,
  i.item_price + COALESCE(i.freight_value, 0) AS item_value_including_freight
FROM `YOUR_PROJECT_ID.olist_stg.order_items` i
JOIN `YOUR_PROJECT_ID.olist_stg.orders` o USING (order_id);
