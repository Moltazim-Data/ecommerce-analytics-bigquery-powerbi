CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.olist_dwh.dim_date` AS
WITH d AS (
  SELECT d AS date_day
  FROM UNNEST(GENERATE_DATE_ARRAY('2016-01-01', '2019-12-31')) d
)
SELECT
  date_day,
  EXTRACT(YEAR FROM date_day) AS year,
  EXTRACT(MONTH FROM date_day) AS month,
  FORMAT_DATE('%Y-%m', date_day) AS year_month,
  EXTRACT(WEEK FROM date_day) AS week,
  EXTRACT(DAYOFWEEK FROM date_day) AS day_of_week
FROM d;
SQL

