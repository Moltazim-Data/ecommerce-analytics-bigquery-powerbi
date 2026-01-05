CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.olist_mart.category_monthly` AS
SELECT
  FORMAT_DATE('%Y-%m', f.purchase_date) AS year_month,
  COALESCE(dp.product_category_name_english, dp.product_category_name, 'unknown') AS category,
  COUNT(DISTINCT f.order_id) AS orders,
  SUM(f.gross_item_value) AS revenue_gross
FROM `YOUR_PROJECT_ID.olist_dwh.fact_order_item` f
LEFT JOIN `YOUR_PROJECT_ID.olist_dwh.dim_product` dp
  ON f.product_id = dp.product_id
GROUP BY year_month, category;
SQL

