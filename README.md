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

1. Create BigQuery datasets in the same region.
2. Load the Olist CSVs into `olist_raw`.
3. Add and test staging SQL with explicit casts, null handling, and deduplication rules.
4. Build a documented star schema and define the grain of each fact table.
5. Create marts and reconcile their totals to source data.
6. Build the Power BI report and commit screenshots or an export only after it exists.
7. Publish an executive summary based on query outputs, not assumed results.

## Expected repository layout

```text
sql/          Ordered SQL scripts for staging, warehouse, and marts
notebooks/    Data quality and exploratory analysis
dashboard/    Genuine dashboard screenshots or exports
reports/      Validated findings and recommendations
docs/         Data dictionary, model diagram, KPI definitions
```

These folders describe the planned deliverables; confirm the actual committed files before assuming a deliverable is complete. Use `YOUR_PROJECT_ID` only as a placeholder and never commit credentials.

## KPI definitions to document

- Revenue/order value: specify source fields, order statuses included, and treatment of freight, cancellations, and refunds.
- Orders: count distinct order IDs, not order lines.
- AOV: explicitly define the revenue numerator and distinct-order denominator.
- Delivery time: define the timestamp pair and handling of undelivered/cancelled orders.
- RFM: document reference date and scoring/segment rules.

## Status and limitations

This is a reproducibility-focused portfolio exercise. Only describe BigQuery tables, Power BI pages, scheduled refresh, or deployed assets as completed after creating and validating them in your own environment. Do not commit service-account keys or other secrets.
