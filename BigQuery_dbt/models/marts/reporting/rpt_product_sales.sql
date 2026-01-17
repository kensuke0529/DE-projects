{{ config(materialized='table') }}

with items as (
  select * from {{ ref('int_purchase_items') }}
),

session_context as (
  select
    session_id,
    country,
    region,
    city
  from {{ ref('rpt_sessions_wide') }}
)

select
  i.purchase_event_key,
  i.user_pseudo_id,
  i.event_date,
  i.purchase_ts,
  
  i.transaction_id,
  i.ga_session_id as session_id,
  
  i.item_id,
  i.item_name,
  i.item_brand,
  i.item_category,
  
  i.price,
  i.quantity,
  i.item_revenue_calc as item_revenue,
  
  s.country,
  s.region,
  s.city

from items i
left join session_context s on i.ga_session_id = s.session_id
