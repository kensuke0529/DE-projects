{{ config(materialized='view') }}

/*
  Purchase Line Items Model
  
  Uses LEFT JOINs to preserve ALL purchase events, even those missing:
  - transaction_id (906 events / 15.9% of purchases)
  - product items (2 events)
  
  Data Quality Flags:
  - is_valid_transaction: TRUE if transaction_id exists
  - has_items: TRUE if product details exist
  - is_complete: TRUE if both conditions met (ready for analytics)
*/

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
  coalesce(p.transaction_id, concat('MISSING_', e.event_id)) as transaction_id,
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
  coalesce(i.quantity, 0) as quantity,
  i.price as unit_price,
  coalesce(i.item_revenue, 0) as item_revenue,
  
  -- Data Quality Flags
  p.transaction_id is not null as is_valid_transaction,
  i.product_key is not null as has_items,
  (p.transaction_id is not null and i.product_key is not null) as is_complete
  
from purchase_events e
left join params p using (event_id)
left join items i using (event_id)
