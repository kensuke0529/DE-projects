{{ config(materialized='table') }}

/*
  Geographic Aggregation Mart
  Daily metrics aggregated by country, region, and city
*/

with sessions as (
  select
    session_date,
    geo.country as country,
    geo.region as region,
    geo.city as city,
    geo.continent as continent,
    geo.sub_continent as sub_continent,
    
    -- Session metrics
    1 as sessions,
    if(is_first_session, 1, 0) as new_user_sessions,
    if(is_converted, 1, 0) as converted_sessions,
    if(is_bounce, 1, 0) as bounced_sessions,
    
    session_duration_seconds,
    page_views,
    product_views,
    add_to_carts,
    purchases,
    session_revenue
    
  from {{ ref('int_sessions') }}
)

select
  session_date,
  country,
  region,
  city,
  continent,
  sub_continent,
  
  -- Session counts
  count(*) as total_sessions,
  sum(new_user_sessions) as new_user_sessions,
  sum(converted_sessions) as converted_sessions,
  sum(bounced_sessions) as bounced_sessions,
  
  -- Rates
  round(safe_divide(sum(converted_sessions), count(*)) * 100, 2) as conversion_rate,
  round(safe_divide(sum(bounced_sessions), count(*)) * 100, 2) as bounce_rate,
  
  -- Engagement metrics
  round(avg(session_duration_seconds), 2) as avg_session_duration_seconds,
  round(avg(page_views), 2) as avg_pages_per_session,
  
  -- E-commerce metrics
  sum(product_views) as total_product_views,
  sum(add_to_carts) as total_add_to_carts,
  sum(purchases) as total_purchases,
  sum(session_revenue) as total_revenue,
  round(safe_divide(sum(session_revenue), sum(converted_sessions)), 2) as avg_order_value

from sessions
group by 1, 2, 3, 4, 5, 6
order by session_date, total_sessions desc
