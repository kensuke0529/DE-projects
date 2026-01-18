{{ config(materialized='view') }}

with events as (
    select
        event_id,
        event_date,
        event_ts,
        event_name,
        user_pseudo_id,
        platform,
        device,
        geo,
        traffic_source
    from {{ ref('stg_events') }}
    where event_name = 'page_view'
),

cleaned as (
    select
        event_id,
        event_date,
        event_ts,
        user_pseudo_id,
        platform,
        device,
        geo,
        traffic_source
    from events
    where user_pseudo_id is not null
      and event_date is not null
)

select * from cleaned