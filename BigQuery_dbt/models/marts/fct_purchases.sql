{{ config(materialized='table') }}

with purchase_items as (
  select
    transaction_id,
    user_pseudo_id,
    event_date,
    purchase_ts,
    currency,
    total_revenue,
    quantity,
    item_revenue
  from {{ ref('int_purchase_items') }}
)

select
  transaction_id,
  user_pseudo_id,
  event_date,
  purchase_ts,
  currency,
  total_revenue,
  count(*) as items_count,
  sum(quantity) as total_quantity
from purchase_items
group by 1, 2, 3, 4, 5, 6
