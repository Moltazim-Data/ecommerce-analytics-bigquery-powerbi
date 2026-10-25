# E-commerce Analytics — BigQuery, SQL, Python & Power BI

This repository is a **project blueprint and starter workspace** for an analytics workflow using the public Olist Brazilian E-Commerce dataset. The intended design is RAW → STAGING → WAREHOUSE → MARTS → BI. Do not interpret the architecture description as proof that every layer has already been implemented or deployed.

## Intended business questions

- How do order value, order count, and average order value change over time?
- Which product categories contribute the most to sales?
- What do repeat-purchase and customer-segmentation patterns look like?
- How do delivery times and order statuses vary?

## Dataset

Olist Brazilian E-Commerce Public Dataset: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

Review the dataset's license and terms before redistributing source files. This repository does not require private customer information or credentials.

## Target architecture

- `olist_raw`: source tables loaded from CSV
- `olist_stg`: typed, cleaned staging tables
- `olist_dwh`: fact and dimension tables
- `olist_mart`: BI-ready aggregate tables

## Recommended implementation order

1. Create datasets `olist_raw`, `olist_stg`, `olist_dwh`, and `olist_mart` in the same region.
2. Load the Olist CSV tables into `olist_raw` using source table names expected by the scripts (`orders`, `order_items`, `customers`, `products`).
3. Replace `YOUR_PROJECT_ID` in `sql/01_staging.sql`, then run it.
4. Run `sql/02_warehouse.sql`, then `sql/03_marts.sql` in order.
5. Run `docs/validation_checks.sql` and review row counts, null keys, duplicates, and negative prices.
6. Reconcile mart totals to source data before building Power BI visuals.
7. Build the Power BI report and commit screenshots or an export only after it exists.
8. Publish an executive summary based on query outputs, not assumed results.

## Expected repository layout

```text
sql/          Ordered SQL scripts for staging, warehouse, and marts
notebooks/    Data quality and exploratory analysis
dashboard/    Genuine dashboard screenshots or exports
reports/      Validated findings and recommendations
docs/         Data dictionary, model diagram, KPI definitions
```

The SQL starter files now committed are `sql/01_staging.sql`, `sql/02_warehouse.sql`, `sql/03_marts.sql`, and `docs/validation_checks.sql`. KPI definitions are in `docs/metric_definitions.md`. These scripts are templates and have not been run against your BigQuery project in this review. Use `YOUR_PROJECT_ID` only as a placeholder and never commit credentials.

## KPI definitions to document

- Revenue/order value: specify source fields, order statuses included, and treatment of freight, cancellations, and refunds.
- Orders: count distinct order IDs, not order lines.
- AOV: explicitly define the revenue numerator and distinct-order denominator.
- Delivery time: define the timestamp pair and handling of undelivered/cancelled orders.
- RFM: document reference date and scoring/segment rules.

## Status and limitations

This is a reproducibility-focused portfolio exercise. Only describe BigQuery tables, Power BI pages, scheduled refresh, or deployed assets as completed after creating and validating them in your own environment. Do not commit service-account keys or other secrets.
