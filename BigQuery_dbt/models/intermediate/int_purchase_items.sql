{{ config(materialized='view') }}

with purchase_events as (
  select 
    event_id,
    user_pseudo_id,
    event_date,
    event_ts
  from {{ ref('stg_events') }}
  where event_name = 'purchase'
),

params as (
  select
    event_id,
    transaction_id,
    value as total_revenue,
    currency
  from {{ ref('stg_event_param_pivot') }}
),

items as (
  select
    event_id,
    coalesce(item_id, item_name) as product_key,
    item_name,
    item_id,
    item_brand,
    item_category,
    quantity,
    price,
    price * quantity as item_revenue
  from {{ ref('stg_items') }}
)

select
  -- Transaction info
  p.transaction_id,
  e.user_pseudo_id,
  e.event_date,
  e.event_ts as purchase_ts,
  
  -- Financial
  p.total_revenue,
  p.currency,
  
  -- Product info
  i.product_key,
  i.item_id,
  i.item_name,
  i.item_brand,
  i.item_category,
  
  -- Metrics
  i.quantity,
  i.price as unit_price,
  i.item_revenue
  
from purchase_events e
join params p using (event_id)
join items i using (event_id)
where p.transaction_id is not null
  and i.product_key is not null
