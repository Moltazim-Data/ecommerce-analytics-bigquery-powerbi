CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.olist_stg.products` AS
SELECT
  product_id,
  product_category_name,
  SAFE_CAST(product_name_lenght AS INT64)        AS product_name_length,
  SAFE_CAST(product_description_lenght AS INT64) AS product_description_length,
  SAFE_CAST(product_photos_qty AS INT64)         AS product_photos_qty,
  SAFE_CAST(product_weight_g AS INT64) AS product_weight_g,
  SAFE_CAST(product_length_cm AS INT64) AS product_length_cm,
  SAFE_CAST(product_height_cm AS INT64) AS product_height_cm,
  SAFE_CAST(product_width_cm AS INT64)  AS product_width_cm
FROM `YOUR_PROJECT_ID.olist_raw.products`
WHERE product_id IS NOT NULL;
SQL

