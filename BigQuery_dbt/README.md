# E-commerce Revenue Intelligence & Funnel Optimization

**From Raw GA4 Events to Board-Ready Dashboards**

![Data Engineering Pipeline Architecture](images/dashboard_v2.png)

[**View Live Looker Studio Dashboard**](https://lookerstudio.google.com/reporting/db8290ae-d10d-45f9-9cdb-1df9a85a5fff)


## Summary
This project transforms raw Google Analytics 4 data into a clean, "Revenue Intelligence" analysis ready tables. By building a robust dbt pipeline on BigQuery, this pipeline turns clickstream data into actionable insights, enabling stakeholders to answer critical questions about **User Acquisition**, **Funnel Drop-offs**, and **Product Performance**.

**Deliverables:**
*   **Unified Dashboard**: A single pane of glass for Revenue, AOV, and Conversion Rates.

*   **Analysis-Ready**: Data pipeline to provide analysis-ready tables for fast, cost-saving tables.



---

## The Business Problem
E-commerce businesses need real-time answers, but raw GA4 data export is a technical bottleneck:
*   **Nested Complexity**: Key data is buried in `event_params` and `items` arrays, making SQL queries complex and slow.

*   **Fragmented Logic**: "Sessions" aren't explicitly defined in raw events, leading to inconsistent definitions across different analyst reports.

**Impact**: Decisions were being made on slow, potentially inaccurate data, with no clear view of the "Why" behind revenue changes.

---

## The Solution: A Validated dbt Pipeline

We implemented a **Modern Data Stack** (dbt + BigQuery) to standardize logic and guarantee data quality.

### 1. The Target Schema 
To power interactive dashboards without lag, we moved away from star schemas requiring heavy joins at query time. instead, we pre-compute "Wide" reporting tables:

*   **`rpt_sessions_wide`**: The "One Row Per Session" master table.
    *   **Enriched Geography**: Country, Region, City (pre-joined).
    *   **Commerce Metrics**: `total_revenue`, `total_transactions`, `has_transaction` flag.
    *   **Session Context**: Start time, duration, session number, device info.
*   **`rpt_product_sales`**: The "One Row Per Line Item" table.
    *   Links individual product performance back to session contexts (Geo, Channel) without expensive re-joins.

### 2. Data Quality & Trust

*   **Standardized Definitions**: "Session" and "Conversion" are defined once in dbt, not copied-pasted across 10 different SQL queries.

---

## Interactive Insights (The "So What?")

The new data model powers the **Looker Studio Dashboard**, enabling instant answers to:

*   **Funnel Analysis**: Identify the **80.3%** drop-off between "View Item" and "Add to Cart" to prioritize PDP improvements.
*   **Regional Performance**: Texas shows a **2.27%** conversion rate, outperforming California (**1.87%**) despite lower traffic volume.
*   **Device Optimization**: Mobile users convert at **1.4%**, slightly edging out Desktop users (**1.3%**), guiding our "Mobile-First" design strategy.

### 4. Data Quality Analysis
*   **Transparency**: The pipeline identified **1,800 purchase lines** (approx. $32k revenue) with missing Transaction IDs.
*   **Action**: Instead of filtering these out, they are flagged in `rpt_purchase_dq_analysis` for stakeholder review, ensuring 100% of revenue data is visible while maintaining data governance.

---
