{{ config(materialized='table') }}

with flat_items as (
  select
    event_date,
    event_name,
    i.item_id,
    i.item_name,
    i.item_brand,
    i.item_category,
    i.price,
    i.quantity,
    
    -- Calculate revenue for this item in this event
    case 
      when event_name = 'purchase' then coalesce(i.item_revenue, i.price * i.quantity, 0)
      else 0 
    end as item_revenue
    
  from {{ ref('stg_events') }}
  cross join unnest(items) as i
  where event_name in ('view_item', 'add_to_cart', 'begin_checkout', 'purchase')
)

select
  event_date,
  item_id,
  item_name,
  item_brand,
  item_category,
  
  countif(event_name = 'view_item') as item_views,
  countif(event_name = 'add_to_cart') as item_add_to_carts,
  countif(event_name = 'begin_checkout') as item_checkouts,
  countif(event_name = 'purchase') as item_purchases,
  
  sum(item_revenue) as item_revenue

from flat_items
group by 1, 2, 3, 4, 5
order by 1 desc, 10 desc
