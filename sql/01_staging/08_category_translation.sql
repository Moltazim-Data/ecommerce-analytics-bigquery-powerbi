CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.olist_stg.category_translation` AS
SELECT
  string_field_0 AS product_category_name,
  string_field_1 AS product_category_name_english
FROM `YOUR_PROJECT_ID.olist_raw.category_translation`
WHERE string_field_0 IS NOT NULL;
SQL

