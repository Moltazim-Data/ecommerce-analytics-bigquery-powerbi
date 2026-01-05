This folder contains the SQL source code and documentation for the BigQuery analytics pipeline.

Each transformation is also stored as an individual `.sql` file under:
- 01_staging/
- 02_dwh/
- 03_marts/

# BigQuery Pipeline — Olist (RAW → STG → DWH → MART)

This document contains **all SQL queries and run steps** used to build the analytics pipeline:

`olist_raw` → `olist_stg` → `olist_dwh` → `olist_mart`

> Replace `YOUR_PROJECT_ID` with your Google Cloud **project id** (example: `imperial-rarity-480417-n4`).

---

## 0) Prerequisites (BigQuery UI)

1. Create these datasets in the **same location** (ex: **EU**):
   - `olist_raw`
   - `olist_stg`
   - `olist_dwh`
   - `olist_mart`
2. Upload the Olist CSVs into `olist_raw` tables (autodetect is fine):
   - `orders`
   - `order_items`
   - `order_payments`
   - `order_reviews`
   - `customers`
   - `sellers`
   - `products`
   - `geolocation` (optional for this pipeline)
   - `category_translation` (CSV header sometimes becomes `string_field_0`, `string_field_1`)

---

## 1) STAGING layer (clean types, rename columns)

### 1.1 `olist_stg.orders`

```sql
CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.olist_stg.orders` AS
SELECT
  order_id,
  customer_id,
  order_status,
  SAFE_CAST(order_purchase_timestamp AS TIMESTAMP) AS order_purchase_ts,
  SAFE_CAST(order_approved_at AS TIMESTAMP) AS order_approved_ts,
  SAFE_CAST(order_delivered_carrier_date AS TIMESTAMP) AS delivered_carrier_ts,
  SAFE_CAST(order_delivered_customer_date AS TIMESTAMP) AS delivered_customer_ts,
  SAFE_CAST(order_estimated_delivery_date AS TIMESTAMP) AS estimated_delivery_ts
FROM `YOUR_PROJECT_ID.olist_raw.orders`
WHERE order_id IS NOT NULL;
```

Validation:

```sql
SELECT
  COUNT(*) AS row_count,
  COUNTIF(order_purchase_ts IS NULL) AS null_purchase_ts
FROM `YOUR_PROJECT_ID.olist_stg.orders`;
```

---

### 1.2 `olist_stg.order_items`

```sql
CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.olist_stg.order_items` AS
SELECT
  order_id,
  SAFE_CAST(order_item_id AS INT64) AS order_item_id,
  product_id,
  seller_id,
  SAFE_CAST(shipping_limit_date AS TIMESTAMP) AS shipping_limit_ts,
  SAFE_CAST(price AS NUMERIC) AS price,
  SAFE_CAST(freight_value AS NUMERIC) AS freight_value
FROM `YOUR_PROJECT_ID.olist_raw.order_items`
WHERE order_id IS NOT NULL;
```

Validation:

```sql
SELECT
  COUNT(*) AS row_count,
  COUNTIF(price IS NULL) AS null_price,
  COUNTIF(freight_value IS NULL) AS null_freight
FROM `YOUR_PROJECT_ID.olist_stg.order_items`;
```

---

### 1.3 `olist_stg.order_payments`

```sql
CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.olist_stg.order_payments` AS
SELECT
  order_id,
  SAFE_CAST(payment_sequential AS INT64) AS payment_sequential,
  payment_type,
  SAFE_CAST(payment_installments AS INT64) AS payment_installments,
  SAFE_CAST(payment_value AS NUMERIC) AS payment_value
FROM `YOUR_PROJECT_ID.olist_raw.order_payments`
WHERE order_id IS NOT NULL;
```

Validation:

```sql
SELECT
  COUNT(*) AS row_count,
  COUNTIF(payment_value IS NULL) AS null_payment_value
FROM `YOUR_PROJECT_ID.olist_stg.order_payments`;
```

---

### 1.4 `olist_stg.customers`

```sql
CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.olist_stg.customers` AS
SELECT
  customer_id,
  customer_unique_id,
  SAFE_CAST(customer_zip_code_prefix AS STRING) AS customer_zip_code_prefix,
  customer_city,
  customer_state
FROM `YOUR_PROJECT_ID.olist_raw.customers`
WHERE customer_id IS NOT NULL;
```

Validation:

```sql
SELECT
  COUNT(*) AS row_count,
  COUNT(DISTINCT customer_id) AS distinct_customer_id
FROM `YOUR_PROJECT_ID.olist_stg.customers`;
```

---

### 1.5 `olist_stg.products`

> Note: The source column names in `olist_raw.products` are typically:
> `product_name_lenght` and `product_description_lenght` (spelling from dataset).

```sql
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
```

---

### 1.6 `olist_stg.sellers`

```sql
CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.olist_stg.sellers` AS
SELECT
  seller_id,
  SAFE_CAST(seller_zip_code_prefix AS STRING) AS seller_zip_code_prefix,
  seller_city,
  seller_state
FROM `YOUR_PROJECT_ID.olist_raw.sellers`
WHERE seller_id IS NOT NULL;
```

---

### 1.7 `olist_stg.order_reviews`

```sql
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
```

---

### 1.8 `olist_stg.category_translation` (handles `string_field_0/1`)

If your raw schema is `string_field_0`, `string_field_1` (autodetect missed headers), use:

```sql
CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.olist_stg.category_translation` AS
SELECT
  string_field_0 AS product_category_name,
  string_field_1 AS product_category_name_english
FROM `YOUR_PROJECT_ID.olist_raw.category_translation`
WHERE string_field_0 IS NOT NULL;
```

If your raw schema already has correct column names, you can use:

```sql
CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.olist_stg.category_translation` AS
SELECT
  product_category_name,
  product_category_name_english
FROM `YOUR_PROJECT_ID.olist_raw.category_translation`
WHERE product_category_name IS NOT NULL;
```

---

## 2) DWH layer (Star Schema: Dimensions + Facts)

### 2.1 `olist_dwh.dim_date`

```sql
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
```

---

### 2.2 `olist_dwh.dim_customer`

```sql
CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.olist_dwh.dim_customer` AS
SELECT
  customer_id,
  customer_unique_id,
  customer_zip_code_prefix,
  customer_city,
  customer_state
FROM `YOUR_PROJECT_ID.olist_stg.customers`;
```

---

### 2.3 `olist_dwh.dim_seller`

```sql
CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.olist_dwh.dim_seller` AS
SELECT
  seller_id,
  seller_zip_code_prefix,
  seller_city,
  seller_state
FROM `YOUR_PROJECT_ID.olist_stg.sellers`;
```

---

### 2.4 `olist_dwh.dim_product` (with category translation)

```sql
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
```

---

### 2.5 `olist_dwh.fact_order_item` (main revenue fact)

```sql
CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.olist_dwh.fact_order_item` AS
SELECT
  oi.order_id,
  oi.order_item_id,
  o.customer_id,
  oi.product_id,
  oi.seller_id,
  DATE(o.order_purchase_ts) AS purchase_date,
  o.order_status,
  oi.shipping_limit_ts,
  oi.price,
  oi.freight_value,
  (oi.price + oi.freight_value) AS gross_item_value
FROM `YOUR_PROJECT_ID.olist_stg.order_items` oi
JOIN `YOUR_PROJECT_ID.olist_stg.orders` o
  ON oi.order_id = o.order_id;
```

Validation:

```sql
SELECT
  COUNT(*) AS row_count,
  COUNT(DISTINCT order_id) AS distinct_orders,
  SUM(gross_item_value) AS gross_revenue
FROM `YOUR_PROJECT_ID.olist_dwh.fact_order_item`;
```

---

### 2.6 `olist_dwh.fact_payment`

```sql
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
```

---

### 2.7 `olist_dwh.fact_review` (optional)

```sql
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
```

---

## 3) MART layer (BI-ready, aggregated tables)

### 3.1 `olist_mart.sales_daily`

```sql
CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.olist_mart.sales_daily` AS
SELECT
  purchase_date,
  COUNT(DISTINCT order_id) AS orders,
  COUNT(DISTINCT customer_id) AS customers,
  SUM(price) AS revenue_items,
  SUM(freight_value) AS revenue_freight,
  SUM(gross_item_value) AS revenue_gross,
  SAFE_DIVIDE(SUM(gross_item_value), COUNT(DISTINCT order_id)) AS aov
FROM `YOUR_PROJECT_ID.olist_dwh.fact_order_item`
GROUP BY purchase_date;
```

---

### 3.2 `olist_mart.category_monthly`

```sql
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
```

---

### 3.3 `olist_mart.rfm` (customer segmentation)

**RFM = Recency / Frequency / Monetary**

```sql
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
```

---

## 4) Recommended Run Order

1. **STG**
   - `olist_stg.orders`
   - `olist_stg.order_items`
   - `olist_stg.order_payments`
   - `olist_stg.customers`
   - `olist_stg.products`
   - `olist_stg.sellers`
   - `olist_stg.order_reviews`
   - `olist_stg.category_translation`
2. **DWH**
   - `dim_date`, `dim_customer`, `dim_seller`, `dim_product`
   - `fact_order_item`, `fact_payment`, `fact_review` (optional)
3. **MART**
   - `sales_daily`, `category_monthly`, `rfm`

---

## 5) Notes / Troubleshooting

- **BigQuery locations must match**: `olist_raw`, `olist_stg`, `olist_dwh`, `olist_mart` must be in the **same location** (EU/US).
- If a CSV load fails with “too many errors”, enable:
  - **Allow quoted newlines**
  - Increase **max bad records**
- If `category_translation` columns are `string_field_0/1`, use the staging rename query above.

---
