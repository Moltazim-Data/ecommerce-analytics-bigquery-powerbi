CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.olist_dwh.fact_review` AS
SELECT
  r.review_id,
  r.order_id,
  o.customer_id,
  DATE(o.order_purchase_ts) AS purchase_date,
  r.review_score,
  r.review_creation_ts,
  r.review_answer_ts
FROM `YOUR_PROJECT_ID.olist_stg.order_reviews` r
JOIN `YOUR_PROJECT_ID.olist_stg.orders` o
  ON r.order_id = o.order_id;
SQL

