{{ config(materialized='table') }}

with purchase_items as (
  select
    transaction_id,
    purchase_event_key, -- Add event key
    user_pseudo_id,
    event_date,
    purchase_ts,
    currency,
    ga_session_id,
    event_value as total_revenue, 
    quantity,
    item_revenue_calc as item_revenue
  from {{ ref('int_purchase_items') }}
)

select
  coalesce(transaction_id, concat('(no_id)-', purchase_event_key)) as transaction_id, -- Generate dummy ID if missing
  purchase_event_key,
  user_pseudo_id,
  ga_session_id,
  event_date,
  purchase_ts,
  currency,
  coalesce(max(total_revenue), sum(item_revenue)) as total_revenue,
  count(*) as items_count,
  sum(quantity) as total_quantity
from purchase_items
group by 1, 2, 3, 4, 5, 6, 7
