{{ config(materialized='view') }}

with exploded as (
  select
    e.event_key,
    e.event_date,
    e.event_ts,
    e.user_pseudo_id,
    e.event_name,
    
    i.item_id,
    i.item_name,
    i.item_brand,
    i.item_variant,
    i.item_category,
    i.item_category2,
    i.item_category3,
    i.item_category4,
    i.item_category5,
    i.price,
    i.quantity,
    i.item_revenue

  from {{ ref('stg_events') }} e
  left join unnest(e.items) as i

  -- only keep events that actually have items
  where array_length(e.items) > 0
)

select * from exploded
