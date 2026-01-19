# dbt SQL Models Overview

This document provides a technical breakdown of the dbt models used in our E-commerce Analytics pipeline.

---

## 🏗️ Staging Layer (`models/staging`)
**Goal**: Clean, raw 1:1 reflections of the source data. No business logic, just renaming and type casting.

| Model | Source | Description |
| :--- | :--- | :--- |
| **`stg_events`** | `events_*` | Base event table. One row per event. Cleaned timestamps and unpacked top-level columns. |
| **`stg_event_params`** | `event_params` | Flattened view of the `event_params` array. Critical for extracting custom event data (page_url, session_id). |
| **`stg_items`** | `items` | Flattened view of the `items` array. Contains product details for every event (view, add_to_cart, purchase). |

---

## 🧩 Intermediate Layer (`models/intermediate`)
**Goal**: Specialized transformation subsets. Heavy lifting happens here to prepare for final marts.

| Model | Key Logic | Description |
| :--- | :--- | :--- |
| **`int_sessions`** | `group by session_id` | Aggregates events into **Sessions**. Calculates start/end, duration, and flags (is_bounce, has_purchase). |
| **`int_purchase_items`** | `unnest(items)` | Specific focus on **Purchased Items**. Joins items to transaction-level revenue to create a clean "Sales Line Item" table. |
| **`int_event_param_pivot`** | `pivot()` | Pivots key event parameters (session_id, transaction_id) from the vertical param table back into horizontal columns for easier joining. |
| **`int_geo` / `int_platform`** | Dimensions | Extracts and dedupes distinct User and Device dimensions. |

---

## 🏆 Marts Layer (`models/marts`)
**Goal**: Business-ready, high-performance tables for Reporting and Dashboards.

### Core Marts
| Model | Granularity | Description |
| :--- | :--- | :--- |
| **`fct_sessions`** | One row per Session | The clean session fact table. Used for traffic analysis, retention, and funnel entry. |
| **`fct_purchases`** | One row per Transaction | Authenticated finance table. Use this for official Sales and Order Volume numbers. |
| **`fct_daily_web_metrics`** | One row per Day | Pre-aggregated daily KPI table (Sessions, Conversions, Revenue) for trend analysis. |

### Reporting Marts (`models/marts/reporting`)
*Optimized "Wide Tables" for Looker Studio*

| Model | Description | Usage |
| :--- | :--- | :--- |
| **`rpt_sessions_wide`** | **The Boardroom Table**. Joins Sessions + Geo + Transaction totals. | Powers the main Dashboard filters (Region, Traffic Source) and Conversion Rates. |
| **`rpt_product_sales`** | **The Merch Table**. Joins Sales Items + Session Context. | Powers "Top Selling Products" and "Revenue by Category" charts. |
| **`rpt_funnel_sessions`** | **The Funnel Table**. Session-level funnel progression with stage flags and drop-off analysis. | Powers funnel visualization, conversion optimization, and drop-off analysis dashboards. |

---

## 📐 Key Design Patterns

### 1. The "Wide Table" Strategy
For BI tools like Looker Studio, we favor pre-joining dimensions in the `reporting` layer (e.g., `rpt_sessions_wide`). This avoids slow runtime joins and ensures filters (like "Region = California") work instantly across all metrics.


