{{ config(materialized='table') }}

select 
    session_date, 
    1 as stage_order, 
    'Product views' as stage, 
    sessions_with_product_views as sessions
from {{ ref('agg_daily_sessions') }}

union all
select 
    session_date, 
    2, 
    'Add to cart', 
    sessions_with_add_to_cart
from {{ ref('agg_daily_sessions') }}

union all
select 
    session_date, 
    3, 
    'Checkout', 
    sessions_with_checkout
from {{ ref('agg_daily_sessions') }}

union all
select 
    session_date, 
    4, 
    'Purchase', 
    converted_sessions
from {{ ref('agg_daily_sessions') }}
