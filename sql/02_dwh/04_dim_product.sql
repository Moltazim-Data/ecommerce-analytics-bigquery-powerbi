CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.olist_dwh.dim_product` AS
SELECT
  p.product_id,
  p.product_category_name,
  ct.product_category_name_english,
  p.product_name_length,
  p.product_description_length,
  p.product_photos_qty,
  p.product_weight_g,
  p.product_length_cm,
  p.product_height_cm,
  p.product_width_cm
FROM `YOUR_PROJECT_ID.olist_stg.products` p
LEFT JOIN `YOUR_PROJECT_ID.olist_stg.category_translation` ct
  ON p.product_category_name = ct.product_category_name;
SQL

