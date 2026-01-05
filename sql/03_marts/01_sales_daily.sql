CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.olist_mart.sales_daily` AS
SELECT
  purchase_date,
  COUNT(DISTINCT order_id) AS orders,
  COUNT(DISTINCT customer_id) AS customers,
  SUM(price) AS revenue_items,
  SUM(freight_value) AS revenue_freight,
  SUM(gross_item_value) AS revenue_gross,
  SAFE_DIVIDE(SUM(gross_item_value), COUNT(DISTINCT order_id)) AS aov
FROM `YOUR_PROJECT_ID.olist_dwh.fact_order_item`
GROUP BY purchase_date;
SQL

