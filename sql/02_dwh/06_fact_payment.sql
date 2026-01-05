CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.olist_dwh.fact_payment` AS
SELECT
  p.order_id,
  o.customer_id,
  DATE(o.order_purchase_ts) AS purchase_date,
  p.payment_sequential,
  p.payment_type,
  p.payment_installments,
  p.payment_value,
  o.order_status
FROM `YOUR_PROJECT_ID.olist_stg.order_payments` p
JOIN `YOUR_PROJECT_ID.olist_stg.orders` o
  ON p.order_id = o.order_id;
SQL

