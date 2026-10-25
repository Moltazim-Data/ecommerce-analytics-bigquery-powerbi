-- BigQuery Standard SQL
-- These marts are descriptive. Item value includes freight and excludes cancelled orders.
CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.olist_mart.monthly_sales` AS
SELECT
  DATE_TRUNC(order_date, MONTH) AS month,
  COUNT(DISTINCT order_id) AS orders,
  COUNT(*) AS order_items,
  SUM(item_value_including_freight) AS item_value_including_freight
FROM `YOUR_PROJECT_ID.olist_dwh.fact_order_items`
WHERE order_status NOT IN ('canceled', 'unavailable')
GROUP BY month;

CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.olist_mart.category_sales` AS
SELECT
  COALESCE(p.product_category_name, 'unknown') AS product_category_name,
  COUNT(DISTINCT f.order_id) AS orders,
  SUM(f.item_price) AS item_sales_excluding_freight,
  SUM(f.freight_value) AS freight_value
FROM `YOUR_PROJECT_ID.olist_dwh.fact_order_items` f
LEFT JOIN `YOUR_PROJECT_ID.olist_dwh.dim_products` p USING (product_id)
WHERE f.order_status NOT IN ('canceled', 'unavailable')
GROUP BY product_category_name;

CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.olist_mart.customer_rfm` AS
WITH customer_orders AS (
  SELECT customer_id, order_id, order_date,
         SUM(item_value_including_freight) AS order_value
  FROM `YOUR_PROJECT_ID.olist_dwh.fact_order_items`
  WHERE order_status NOT IN ('canceled', 'unavailable')
  GROUP BY customer_id, order_id, order_date
), reference_date AS (
  SELECT DATE_ADD(MAX(order_date), INTERVAL 1 DAY) AS as_of_date FROM customer_orders
)
SELECT
  customer_id,
  DATE_DIFF((SELECT as_of_date FROM reference_date), MAX(order_date), DAY) AS recency_days,
  COUNT(DISTINCT order_id) AS frequency,
  SUM(order_value) AS monetary_value_including_freight
FROM customer_orders
GROUP BY customer_id;
