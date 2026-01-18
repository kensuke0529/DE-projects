{{ config(materialized='table') }}

with daily as (
  select
    session_date,
    sessions_with_product_views,
    sessions_with_add_to_cart,
    sessions_with_checkout,
    converted_sessions
  from {{ ref('agg_daily_sessions') }}
),

weekly as (
  select
    date_trunc(session_date, week(monday)) as week_start,

    sum(sessions_with_product_views) as sessions_with_product_views,
    sum(sessions_with_add_to_cart) as sessions_with_add_to_cart,
    sum(sessions_with_checkout) as sessions_with_checkout,
    sum(converted_sessions) as converted_sessions
  from daily
  group by 1
),

rates as (
  select
    week_start,
    sessions_with_product_views,
    sessions_with_add_to_cart,
    sessions_with_checkout,
    converted_sessions,

    1 - safe_divide(sessions_with_add_to_cart, sessions_with_product_views) as dropoff_view_to_cart,
    1 - safe_divide(sessions_with_checkout, sessions_with_add_to_cart) as dropoff_cart_to_checkout,
    1 - safe_divide(converted_sessions, sessions_with_checkout) as dropoff_checkout_to_purchase
  from weekly
)

select * from rates
