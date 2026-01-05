# E-commerce Analytics (BigQuery + SQL + Power BI)

End-to-end analytics project using the Olist Brazilian E-Commerce dataset.
The goal is to build a complete analytics workflow: raw data → cleaned staging → star schema (DWH) → analytics marts → Power BI dashboard → business insights.

## Project Highlights
- Built a layered analytics architecture (RAW → STG → DWH → MART)
- Designed a star schema with fact and dimension tables
- Created BI-ready data marts for sales, categories, and RFM segmentation
- Focused on reproducibility and SQL-first data modeling

## Tech Stack
- BigQuery (data warehouse)
- SQL (staging, star schema, analytics marts)
- Python (data quality checks, exploratory analysis in Jupyter)
- Power BI (dashboard)

## Dataset
Olist Brazilian E-Commerce Public Dataset (Kaggle):
- https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

## Architecture
**Raw → Staging → DWH → Marts**
- `olist_raw`: raw CSV tables loaded to BigQuery
- `olist_stg`: cleaned/typed staging tables
- `olist_dwh`: star schema (dimensions + facts)
- `olist_mart`: dashboard-ready analytics tables

## Business Questions
- How do revenue, orders, customers, and AOV evolve over time?
- Which categories and products generate the most revenue?
- What is the repeat customer rate and cohort retention over time?
- Which customer segments are most valuable (RFM)?
- What operational patterns appear (delivery times, cancellations if available)?

## Deliverables
- BigQuery SQL scripts (staging, DWH, marts)
- Data quality & EDA notebooks
- Power BI dashboard (screenshots + PDF export)
- Executive summary with key insights and recommendations

## Repository Structure
- `sql/` : BigQuery SQL scripts (staging, DWH, marts) with execution order documented
- `notebooks/` : data quality + EDA
- `dashboard/` : Power BI export + screenshots
- `reports/` : executive summary
- `docs/` : data dictionary and notes

## How to Reproduce (High Level)
1. Download the dataset from Kaggle.
2. Create BigQuery datasets: `olist_raw`, `olist_stg`, `olist_dwh`, `olist_mart` (EU region recommended).
3. Upload CSVs into `olist_raw`.
4. Run SQL scripts in order from `sql/`.
5. Connect Power BI to BigQuery marts and build the dashboard.

> Note: SQL scripts use `YOUR_PROJECT_ID` as a placeholder.  
> Replace it with your own Google Cloud project ID when running the queries.

