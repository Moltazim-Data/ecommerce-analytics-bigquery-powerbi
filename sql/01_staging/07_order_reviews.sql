CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.olist_stg.order_reviews` AS
SELECT
  review_id,
  order_id,
  SAFE_CAST(review_score AS INT64) AS review_score,
  review_comment_title,
  review_comment_message,
  SAFE_CAST(review_creation_date AS TIMESTAMP) AS review_creation_ts,
  SAFE_CAST(review_answer_timestamp AS TIMESTAMP) AS review_answer_ts
FROM `YOUR_PROJECT_ID.olist_raw.order_reviews`
WHERE review_id IS NOT NULL AND order_id IS NOT NULL;
SQL

