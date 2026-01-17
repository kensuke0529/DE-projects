{{ config(materialized='view') }}

with session_aggregates as (
  select
    -- Session identifiers
    e.user_pseudo_id,
    p.ga_session_id,
    concat(e.user_pseudo_id, '-', cast(p.ga_session_id as string), '-', cast(e.event_date as string)) as session_key,

    -- Session timing
    e.event_date as session_date,
    min(e.event_ts) as session_start_ts,
    max(e.event_ts) as session_end_ts,
    timestamp_diff(max(e.event_ts), min(e.event_ts), second) as session_duration_seconds,

    -- Session sequence
    max(p.ga_session_number) as session_number,

    -- Platform
    array_agg(e.operating_system order by e.event_ts limit 1)[safe_offset(0)] as operating_system,
    array_agg(e.browser order by e.event_ts limit 1)[safe_offset(0)] as browser,
    array_agg(e.device_category order by e.event_ts limit 1)[safe_offset(0)] as device_category,
    array_agg(e.platform order by e.event_ts limit 1)[safe_offset(0)] as platform,
    array_agg(e.city order by e.event_ts limit 1)[safe_offset(0)] as city,
    array_agg(e.country order by e.event_ts limit 1)[safe_offset(0)] as country,
    array_agg(e.region order by e.event_ts limit 1)[safe_offset(0)] as region,

    -- Landing page
    array_agg(p.page_location order by e.event_ts limit 1)[safe_offset(0)] as landing_page,

    -- Exit page
    array_agg(p.page_location order by e.event_ts desc limit 1)[safe_offset(0)] as exit_page,

    -- Referrer
    array_agg(p.page_referrer order by e.event_ts limit 1)[safe_offset(0)] as referrer,

    -- Event counts
    count(*) as total_events,
    countif(e.event_name = 'page_view') as page_views,
    countif(e.event_name = 'scroll') as scrolls,
    countif(e.event_name = 'click') as clicks,
    countif(e.event_name = 'view_item') as product_views,
    countif(e.event_name = 'add_to_cart') as add_to_carts,
    countif(e.event_name = 'begin_checkout') as checkouts_started,
    countif(e.event_name = 'purchase') as purchases,

    -- Revenue
    sum(if(e.event_name = 'purchase', p.value_param, 0)) as session_revenue,
    max(p.currency) as currency,

    -- Transaction IDs
    array_agg(distinct p.transaction_id ignore nulls) as transaction_ids

  from {{ ref('stg_events') }} e
  left join {{ ref('int_event_param_pivot') }} p using (event_key)
  where p.ga_session_id is not null
  group by e.user_pseudo_id, p.ga_session_id, e.event_date
)

select
  *,
  purchases > 0 as is_converted,
  session_number = 1 as is_first_session,
  page_views = 1 as is_bounce
from session_aggregates
