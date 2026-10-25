# Metric definitions

| Metric | Definition | Caveat |
|---|---|---|
| Orders | Distinct `order_id` values in eligible orders | Exclude canceled/unavailable orders where documented |
| Order items | Rows at order-item grain | Not the same as distinct orders |
| Item sales | Sum of item price | Excludes freight |
| Item value incl. freight | Item price + freight value | Not profit; taxes, refunds, and all accounting adjustments are not modeled |
| Average order value | Defined as the selected order-value numerator divided by distinct eligible orders | State whether freight is included |
| RFM recency | Days since last eligible purchase relative to max purchase date + one day | Relative to dataset cutoff, not today's date |
| RFM frequency | Distinct eligible orders per customer ID | Customer ID may differ from unique-person customer ID |

## Data modeling notes

- `fact_order_items` is at one row per order item; avoid summing order-level measures after joining to this table without considering fanout.
- Validate row counts, null keys, duplicate keys, and join coverage after loading source tables.
- The starter marts exclude canceled and unavailable statuses but do not model refunds, returns, or profit.
- Product category values may be missing; the mart labels missing categories as `unknown`.
