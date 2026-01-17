{{ config(materialized='table') }}

with sessions_daily as (
  select
    event_date,
    count(distinct session_id) as total_sessions,
    count(distinct user_pseudo_id) as total_users,
    avg(session_length) as average_session_duration
  from {{ ref('fct_sessions') }}
  group by 1
),

revenue_daily as (
  select
    event_date,
    count(distinct transaction_id) as total_transactions,
    sum(total_revenue) as total_revenue,
    sum(total_quantity) as total_quantity
  from {{ ref('fct_purchases') }}
  group by 1
)

select
  s.event_date,
  s.total_sessions,
  s.total_users,
  s.average_session_duration,
  coalesce(r.total_transactions, 0) as total_transactions,
  coalesce(r.total_revenue, 0.0) as total_revenue,
  coalesce(r.total_quantity, 0) as total_quantity,
  ifnull(safe_divide(r.total_transactions, s.total_sessions), 0.0) as conversion_rate
from sessions_daily s
left join revenue_daily r using (event_date)
