CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.olist_mart.rfm` AS
WITH base AS (
  SELECT
    customer_id,
    MAX(purchase_date) AS last_purchase_date,
    COUNT(DISTINCT order_id) AS frequency,
    SUM(gross_item_value) AS monetary
  FROM `YOUR_PROJECT_ID.olist_dwh.fact_order_item`
  GROUP BY customer_id
),
scored AS (
  SELECT
    *,
    DATE_DIFF(
      (SELECT MAX(purchase_date) FROM `YOUR_PROJECT_ID.olist_dwh.fact_order_item`),
      last_purchase_date,
      DAY
    ) AS recency_days,
    NTILE(5) OVER (
      ORDER BY DATE_DIFF(
        (SELECT MAX(purchase_date) FROM `YOUR_PROJECT_ID.olist_dwh.fact_order_item`),
        last_purchase_date,
        DAY
      ) DESC
    ) AS r_score,
    NTILE(5) OVER (ORDER BY frequency) AS f_score,
    NTILE(5) OVER (ORDER BY monetary) AS m_score
  FROM base
)
SELECT
  customer_id,
  recency_days,
  frequency,
  monetary,
  r_score,
  f_score,
  m_score,
  CONCAT(CAST(r_score AS STRING), CAST(f_score AS STRING), CAST(m_score AS STRING)) AS rfm_score
FROM scored;
SQL

