# dbt Core + BigQuery: GA4 E-commerce Analytics Pipeline

A complete data transformation pipeline built with **dbt Core** and **Google BigQuery**, transforming raw Google Analytics 4 (GA4) e-commerce data into analytics-ready models.

## Project Overview

This project demonstrates a production-ready dbt data pipeline that transforms the [Google Analytics 4 sample e-commerce dataset](https://developers.google.com/analytics/bigquery/web-ecommerce-demo-dataset) into clean, structured data models for business analytics.

The pipeline follows the **medallion architecture** with clear separation between staging, intermediate, and mart layers—making it easy to maintain, test, and extend.

---

## Data Source

**Source**: `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`

This public dataset contains GA4 event data from a real e-commerce website, including:
- Page views — user interactions
- Product views — add-to-cart events
- Checkout — purchase transactions
- User sessions with device/geo information

**Date Range**: November 1, 2020 – January 31, 2021

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         BigQuery Public Data                        │
│                    ga4_obfuscated_sample_ecommerce                  │
└────────────────────────────────┬────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        STAGING LAYER (Views)                        │
├─────────────────┬──────────────────┬───────────────┬────────────────┤
│   stg_events    │  stg_event_params │   stg_items   │ stg_event_     │
│                 │                   │               │ param_pivot    │
└────────┬────────┴────────┬─────────┴───────┬───────┴────────────────┘
         │                 │                 │
         ▼                 ▼                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     INTERMEDIATE LAYER (Views)                      │
├─────────────────────────┬─────────────────────┬─────────────────────┤
│      int_sessions       │   int_purchase_items │    int_page_views   │
│  (session aggregates)   │  (transaction line   │  (page view details)│
│                         │       items)         │                     │
└────────────┬────────────┴──────────┬──────────┴─────────────────────┘
             │                       │
             ▼                       ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        MARTS LAYER (Tables)                         │
├────────────────┬──────────────┬──────────────────┬──────────────────┤
│agg_daily_      │fct_purchases │   dim_products   │    dim_views     │
│sessions        │              │                  │                  │
├────────────────┴──────────────┴──────────────────┴──────────────────┤
│       agg_daily_product_revenue       │    agg_daily_funnel_long   │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Model Layers

### Staging Layer
Lightweight views that clean — standardize raw GA4 data:

| Model | Description |
|-------|-------------|
| `stg_events` | Core event data with parsed dates, timestamps, and unique event IDs |
| `stg_event_params` | Flattened event parameters from nested arrays |
| `stg_items` | Product/item data extracted from purchase events |
| `stg_event_param_pivot` | Pivoted parameters with ga_session_id, page_location, transaction_id, etc. |

### Intermediate Layer
Business logic transformations that join — aggregate staging models:

| Model | Description |
|-------|-------------|
| `int_sessions` | Session-level aggregates with engagement metrics, conversion flags, and revenue |
| `int_purchase_items` | Transaction line items with product details and calculated revenue |
| `int_page_views` | Page view events enriched with session context |

### Marts Layer
Analytics-ready tables optimized for reporting — dashboards:

| Model | Description |
|-------|-------------|
| `fct_purchases` | Transaction-level fact table with item counts and revenue |
| `dim_products` | Product dimension with unique products from the catalog |
| `dim_views` | Daily page view aggregates with unique user counts |
| `agg_daily_sessions` | Daily session metrics including conversion rates, bounce rates, and revenue |
| `agg_daily_product_revenue` | Daily product performance with revenue aggregation |
| `agg_daily_funnel_long` | Funnel analysis in long format: product views — add to cart — checkout — purchase |

---

## Key Analytics Features

### Session Analytics (`agg_daily_sessions`)
- **User Engagement**: Session duration, pages per session, bounce rate
- **New vs Returning**: Tracks first-time vs repeat visitors
- **Conversion Funnel**: Product views — Add to cart — Checkout — Purchase
- **Revenue Metrics**: Total revenue, average order value, revenue per session

### E-commerce Analytics
- **Transaction Tracking**: Full purchase history with line-item detail
- **Product Performance**: Daily revenue by product
- **User Behavior**: Session-level engagement and conversion tracking

---

## Data Quality

All models include schema tests defined in `schema.yml` files:

- **Primary Keys**: Uniqueness and not-null constraints
- **Required Fields**: Not-null tests on critical columns
- **Referential Integrity**: Relationship tests between models

---

## Technical Highlights

- **Wildcard Table Handling**: Uses `_TABLE_SUFFIX` to efficiently query date-sharded GA4 tables
- **Configurable Date Range**: Date range controlled via dbt variables (`ga4_start_suffix`, `ga4_end_suffix`)
- **Event ID Generation**: SHA256 hash creates unique event identifiers
- **Nested Data Handling**: Proper unnesting of GA4's nested event_params and items arrays
- **Materialization Strategy**: Views for staging/intermediate (cost-efficient), tables for marts (query performance)

---

## Project Structure

```
BigQuery_dbt/
├── models/
│   ├── source/           # Source definitions (GA4 public dataset)
│   ├── staging/          # Data cleaning — standardization
│   ├── intermediate/     # Business logic — aggregations
│   └── marts/            # Analytics-ready output tables
├── macros/               # Custom Jinja macros
├── dbt_project.yml       # Project configuration
└── packages.yml          # dbt package dependencies
```

---

## Documentation

This project includes auto-generated dbt documentation with:
- Model lineage graphs showing data flow
- Column descriptions and data types
- Test coverage and data quality rules

Generate and view documentation locally with:
```bash
dbt docs generate && dbt docs serve
```

---

## Live Dashboard

Explore the analytics visually with the **Looker Studio Dashboard**:

**[View Dashboard](https://lookerstudio.google.com/reporting/ff72ef08-e8a3-4938-872f-98c45a86ab26)**

![Looker Studio Dashboard](images/dashboard.png)

This interactive dashboard connects directly to the BigQuery marts layer and provides:
- Daily session and conversion metrics
- Product performance analytics
- E-commerce funnel visualization
- Revenue trends and insights
