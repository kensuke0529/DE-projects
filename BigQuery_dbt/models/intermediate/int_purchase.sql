{{ config(materialized='view') }}

with events as (
  select *
  from {{ ref('stg_events') }}
  where event_name = 'purchase'
),

params as (
  select *
  from {{ ref('stg_event_param_pivot') }}
)

select
  p.transaction_id,
  e.user_pseudo_id,
  p.ga_session_id,
  e.event_ts as purchase_ts,
  p.value as revenue,
  p.currency
from events e
join params p
  using (event_id)
where p.transaction_id is not null
