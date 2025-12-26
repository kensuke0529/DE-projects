{{ config(materialized='view') }}

/*
  Intermediate Session Model
  Aggregates events into sessions using GA4's ga_session_id
  Each row = one user session with aggregated metrics
*/

with events as (
  select
    e.event_id,
    e.user_pseudo_id,
    e.event_date,
    e.event_ts,
    e.event_name,
    e.platform,
    e.device,
    e.geo,
    e.traffic_source,
    
    p.ga_session_id,
    p.ga_session_number,
    p.page_location,
    p.page_referrer,
    p.transaction_id,
    p.value,
    p.currency
  from {{ ref('stg_events') }} e
  left join {{ ref('stg_event_param_pivot') }} p using (event_id)
  where p.ga_session_id is not null
),

session_aggregates as (
  select
    -- Session identifiers
    user_pseudo_id,
    ga_session_id,
    concat(user_pseudo_id, '-', cast(ga_session_id as string)) as session_key,
    
    -- Session timing
    event_date as session_date,
    min(event_ts) as session_start_ts,
    max(event_ts) as session_end_ts,
    timestamp_diff(max(event_ts), min(event_ts), second) as session_duration_seconds,
    
    -- Session sequence
    max(ga_session_number) as session_number,
    
    -- Platform & Device (first touch)
    array_agg(platform order by event_ts limit 1)[safe_offset(0)] as platform,
    array_agg(device order by event_ts limit 1)[safe_offset(0)] as device,
    array_agg(geo order by event_ts limit 1)[safe_offset(0)] as geo,
    array_agg(traffic_source order by event_ts limit 1)[safe_offset(0)] as traffic_source,
    
    -- Landing page (first page viewed)
    array_agg(page_location order by event_ts limit 1)[safe_offset(0)] as landing_page,
    
    -- Exit page (last page viewed)
    array_agg(page_location order by event_ts desc limit 1)[safe_offset(0)] as exit_page,
    
    -- Referrer
    array_agg(page_referrer order by event_ts limit 1)[safe_offset(0)] as referrer,
    
    -- Event counts
    count(*) as total_events,
    countif(event_name = 'page_view') as page_views,
    countif(event_name = 'scroll') as scrolls,
    countif(event_name = 'click') as clicks,
    countif(event_name = 'view_item') as product_views,
    countif(event_name = 'add_to_cart') as add_to_carts,
    countif(event_name = 'begin_checkout') as checkouts_started,
    countif(event_name = 'purchase') as purchases,
    
    -- Revenue
    sum(if(event_name = 'purchase', value, 0)) as session_revenue,
    max(currency) as currency,
    
    -- Transaction IDs
    array_agg(distinct transaction_id ignore nulls) as transaction_ids
    
  from events
  group by user_pseudo_id, ga_session_id, event_date
)

select
  *,
  -- Derived flags
  purchases > 0 as is_converted,
  session_number = 1 as is_first_session,
  page_views = 1 as is_bounce
from session_aggregates
