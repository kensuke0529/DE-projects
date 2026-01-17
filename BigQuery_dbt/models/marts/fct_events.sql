{{ config(materialized='table') }}

with base_events as (
  select
    event_key,
    event_date,
    event_ts,
    user_pseudo_id,
    event_name
  from {{ ref('stg_events') }}
),

event_params as (
  select
    event_key,
    ga_session_id,
    transaction_id,
    value_param as event_value,
    currency,
    page_location,
    page_title
  from {{ ref('int_event_param_pivot') }}
)

select
  e.event_key,
  e.event_date,
  e.event_ts,
  e.user_pseudo_id,
  e.event_name,
  
  p.ga_session_id,
  p.transaction_id,
  p.event_value,
  p.currency,
  p.page_location,
  p.page_title
from base_events e
left join event_params p using (event_key)
