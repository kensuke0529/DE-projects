{{ config(materialized='table') }}

/*
  Incomplete Purchases Analysis
  
  This table captures purchase events that are missing key data:
  - Missing transaction_id (906 events / $22,932 potential revenue)
  - Missing product items (2 events)
  
  Use this for debugging data quality issues, NOT for analytics.
*/

with incomplete_purchases as (
  select
    transaction_id,
    user_pseudo_id,
    event_date,
    purchase_ts,
    total_revenue,
    currency,
    product_key,
    item_name,
    quantity,
    item_revenue,
    is_valid_transaction,
    has_items,
    is_complete
  from {{ ref('int_purchase_items') }}
  where not is_complete
),

-- Aggregate by date and issue type
daily_summary as (
  select
    event_date,
    count(*) as incomplete_rows,
    countif(not is_valid_transaction) as missing_transaction_id,
    countif(not has_items) as missing_items,
    sum(coalesce(total_revenue, 0)) as potential_revenue_lost
  from incomplete_purchases
  group by event_date
)

-- Return both detail and summary
select
  ip.*,
  ds.incomplete_rows as daily_incomplete_count,
  ds.potential_revenue_lost as daily_potential_revenue
from incomplete_purchases ip
left join daily_summary ds using (event_date)
order by event_date, purchase_ts
