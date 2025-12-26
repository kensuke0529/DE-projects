{{ config(materialized='table') }}

/*
  Daily Session Metrics Mart
  Aggregates session data by date for reporting and dashboards
*/

with sessions as (
  select * from {{ ref('int_sessions') }}
),

daily_metrics as (
  select
    session_date,
    platform,
    
    -- Session counts
    count(*) as total_sessions,
    count(distinct user_pseudo_id) as unique_users,
    
    -- New vs returning
    countif(is_first_session) as new_user_sessions,
    countif(not is_first_session) as returning_user_sessions,
    
    -- Engagement metrics
    avg(session_duration_seconds) as avg_session_duration_seconds,
    avg(page_views) as avg_pages_per_session,
    sum(page_views) as total_page_views,
    
    -- Bounce rate
    countif(is_bounce) as bounced_sessions,
    safe_divide(countif(is_bounce), count(*)) as bounce_rate,
    
    -- Conversion funnel
    countif(product_views > 0) as sessions_with_product_views,
    countif(add_to_carts > 0) as sessions_with_add_to_cart,
    countif(checkouts_started > 0) as sessions_with_checkout,
    countif(is_converted) as converted_sessions,
    
    -- Conversion rates
    safe_divide(countif(is_converted), count(*)) as session_conversion_rate,
    safe_divide(countif(add_to_carts > 0), countif(product_views > 0)) as add_to_cart_rate,
    safe_divide(countif(is_converted), countif(checkouts_started > 0)) as checkout_completion_rate,
    
    -- Revenue
    sum(session_revenue) as total_revenue,
    avg(if(is_converted, session_revenue, null)) as avg_order_value,
    safe_divide(sum(session_revenue), count(*)) as revenue_per_session

  from sessions
  group by session_date, platform
)

select
  *,
  -- User engagement ratio
  safe_divide(returning_user_sessions, new_user_sessions) as returning_to_new_ratio
from daily_metrics
order by session_date desc, platform
