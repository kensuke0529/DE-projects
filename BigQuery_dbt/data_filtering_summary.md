# Data Filtering Summary Report

## Overview

Analysis of the dbt GA4 e-commerce pipeline revealed **1,800 purchase line items** filtered due to incomplete data, representing ~11% of all purchase records.

---

## Key Metrics

| Metric | Value |
|--------|-------|
| **Total Purchase Events** | 5,692 |
| **Complete (analytics-ready)** | 4,786 (84.1%) |
| **Missing transaction_id** | 906 (15.9%) |
| **Missing product items** | 2 (0.04%) |
| **Potential Revenue at Risk** | $22,932.39 |
| **Unique Users Affected** | 788 |

---

## Root Cause Analysis

### Operating System Breakdown (Incomplete Purchases)

| Operating System | Incomplete Count |
|------------------|------------------|
| Web | 538 (59%) |
| iOS | 127 (14%) |
| Windows | 85 (9%) |
| Android | 84 (9%) |
| Macintosh | 57 (6%) |

### Time Pattern

| Finding | Detail |
|---------|--------|
| Peak Period | Nov 1-14, 2020 |
| Post Nov 15 | Drop to ~20-30 per week |

### Weekly Breakdown

| Week Starting | Incomplete Count | Revenue Lost |
|---------------|-----------------|--------------|
| 2020-11-01 | 273 | $13,025.47 |
| 2020-11-08 | 185 | $9,906.92 |
| 2020-11-15 | 20 | — |
| 2020-11-22 | 27 | — |
| 2020-11-29 | 29 | — |

### Likely Cause

**GA4 tracking misconfiguration during initial setup** (Nov 2020). The `transaction_id` event parameter was not being captured correctly during the checkout flow. The issue was largely resolved after November 15, 2020.

---

## Resolution

### Staging Layer Fix

Updated `stg_events.sql` to flatten the `device` STRUCT into queryable columns:

| New Column | Description |
|------------|-------------|
| `operating_system` | OS (Android, iOS, Windows, Macintosh, Web) |
| `device_category` | Device type (mobile, desktop, tablet) |
| `mobile_brand` | Brand (Huawei, Apple, Samsung, etc.) |

### Intermediate Layer Fix

Updated `int_purchase_items.sql`:
- Changed INNER JOINs → LEFT JOINs to preserve all data
- Added data quality flags: `is_valid_transaction`, `has_items`, `is_complete`

### Mart Layer Fix

Added `WHERE is_complete = TRUE` filter to:
- `fct_purchases.sql`
- `agg_daily_product_revenue.sql`
- `dim_products.sql`

### New Tables

- `analysis_incomplete_purchases.sql` - Debug table for incomplete records
- `data_quality_purchase_filtering.sql` - Test to monitor filtering rates

---

## Usage

```sql
-- Analytics queries (complete data only)
SELECT * FROM fct_purchases  -- Already filtered

-- Investigate incomplete purchases
SELECT * FROM analysis_incomplete_purchases

-- Check data quality by OS
SELECT operating_system, COUNT(*) 
FROM stg_events 
WHERE event_name = 'purchase'
GROUP BY 1
```
