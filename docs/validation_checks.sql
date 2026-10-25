-- Run after loading and creating staging tables.
SELECT 'orders_missing_order_id' AS check_name, COUNTIF(order_id IS NULL) AS failures
FROM `YOUR_PROJECT_ID.olist_stg.orders`
UNION ALL
SELECT 'duplicate_order_ids', COUNT(*) FROM (
  SELECT order_id FROM `YOUR_PROJECT_ID.olist_stg.orders`
  GROUP BY order_id HAVING COUNT(*) > 1
)
UNION ALL
SELECT 'order_items_missing_order_id', COUNTIF(order_id IS NULL)
FROM `YOUR_PROJECT_ID.olist_stg.order_items`
UNION ALL
SELECT 'negative_item_prices', COUNTIF(item_price < 0)
FROM `YOUR_PROJECT_ID.olist_stg.order_items`;
