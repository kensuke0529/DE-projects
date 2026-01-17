{{ config(materialized='view') }}

select
  user_pseudo_id,
  min(event_date) as first_seen_date,
  max(event_date) as last_seen_date,
  count(distinct event_date) as days_active,
  count(*) as total_events
from {{ ref('stg_events') }}
group by user_pseudo_id