{{ config(materialized='table') }}

/*
  Funnel Sessions Reporting Table
  
  One row per session with complete funnel progression flags and dimensional enrichment.
  Powers funnel analysis dashboards and conversion optimization reports.
  
  Funnel Stages:
  1. Product Views (view_item)
  2. Add to Cart (add_to_cart)
  3. Checkout (begin_checkout)
  4. Purchase (purchase)
*/

with sessions as (
  select
    user_pseudo_id,
    ga_session_id,
    session_key,
    session_date,
    session_start_ts,
    session_end_ts,
    session_duration_seconds,
    session_number,
    
    -- Platform & Device
    operating_system,
    browser,
    device_category,
    platform,
    device,
    geo,
    traffic_source,
    
    -- Landing/Exit pages
    landing_page,
    exit_page,
    referrer,
    
    -- Funnel event counts
    product_views,
    add_to_carts,
    checkouts_started,
    purchases,
    
    -- Revenue
    session_revenue,
    currency,
    
    -- Session flags
    is_converted,
    is_first_session,
    is_bounce,
    
    -- Transaction IDs
    transaction_ids
    
  from {{ ref('int_sessions') }}
),

-- Extract geo dimensions
geo_enriched as (
  select
    s.*,
    s.geo.country as country,
    s.geo.region as region,
    s.geo.city as city
  from sessions s
),

-- Calculate funnel progression
funnel_progression as (
  select
    *,
    
    -- Funnel stage flags (boolean)
    product_views > 0 as reached_product_view,
    add_to_carts > 0 as reached_add_to_cart,
    checkouts_started > 0 as reached_checkout,
    purchases > 0 as reached_purchase,
    
    -- Furthest stage reached (1-4 scale, 0 if no funnel activity)
    case
      when purchases > 0 then 4
      when checkouts_started > 0 then 3
      when add_to_carts > 0 then 2
      when product_views > 0 then 1
      else 0
    end as furthest_funnel_stage,
    
    -- Furthest stage name
    case
      when purchases > 0 then 'Purchase'
      when checkouts_started > 0 then 'Checkout'
      when add_to_carts > 0 then 'Add to Cart'
      when product_views > 0 then 'Product View'
      else 'No Funnel Activity'
    end as furthest_funnel_stage_name,
    
    -- Drop-off stage (where user stopped in funnel)
    case
      when purchases > 0 then null  -- Completed funnel
      when checkouts_started > 0 then 'Checkout'
      when add_to_carts > 0 then 'Add to Cart'
      when product_views > 0 then 'Product View'
      else null
    end as drop_off_stage,
    
    -- Completion flags
    product_views > 0 and add_to_carts = 0 as dropped_at_product_view,
    add_to_carts > 0 and checkouts_started = 0 as dropped_at_add_to_cart,
    checkouts_started > 0 and purchases = 0 as dropped_at_checkout
    
  from geo_enriched
)

select
  -- Session identifiers
  user_pseudo_id,
  ga_session_id as session_id,
  session_key,
  session_date,
  session_start_ts,
  session_end_ts,
  session_duration_seconds,
  session_number,
  
  -- Session characteristics
  is_first_session,
  is_bounce,
  is_converted,
  
  -- Platform & Device dimensions
  operating_system,
  browser,
  device_category,
  platform,
  device,
  
  -- Geography dimensions
  country,
  region,
  city,
  
  -- Traffic source
  traffic_source.source as traffic_source,
  traffic_source.medium as traffic_medium,
  traffic_source.name as traffic_campaign,
  
  -- Landing/Exit
  landing_page,
  exit_page,
  referrer,
  
  -- Funnel event counts (raw)
  product_views,
  add_to_carts,
  checkouts_started,
  purchases,
  
  -- Funnel progression flags (boolean)
  reached_product_view,
  reached_add_to_cart,
  reached_checkout,
  reached_purchase,
  
  -- Funnel progression metrics
  furthest_funnel_stage,
  furthest_funnel_stage_name,
  drop_off_stage,
  
  -- Drop-off analysis flags
  dropped_at_product_view,
  dropped_at_add_to_cart,
  dropped_at_checkout,
  
  -- Revenue
  session_revenue,
  currency,
  
  -- Transaction details
  transaction_ids,
  array_length(transaction_ids) as transaction_count

from funnel_progression
