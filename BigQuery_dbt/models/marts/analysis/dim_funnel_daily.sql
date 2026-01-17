{{ config(materialized='table') }}

with daily_aggregates as (
  select
    session_date,
    country,
    platform,
    device_category,
    
    count(distinct ga_session_id) as total_sessions,
    
    count(distinct if(product_views > 0, ga_session_id, null)) as sessions_view_item,
    count(distinct if(add_to_carts > 0, ga_session_id, null)) as sessions_add_to_cart,
    count(distinct if(checkouts_started > 0, ga_session_id, null)) as sessions_begin_checkout,
    count(distinct if(purchases > 0, ga_session_id, null)) as sessions_purchase,
    
    sum(session_revenue) as total_revenue

  from {{ ref('int_sessions') }}
  group by 1, 2, 3, 4
)

select
  session_date,
  country,
  platform,
  device_category,
  total_sessions,
  
  sessions_view_item,
  sessions_add_to_cart,
  sessions_begin_checkout,
  sessions_purchase,
  
  total_revenue,
  
  safe_divide(sessions_purchase, total_sessions) as session_conversion_rate
  
from daily_aggregates
order by 1 desc, 2, 3
