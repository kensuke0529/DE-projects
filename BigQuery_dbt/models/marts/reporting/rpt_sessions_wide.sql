{{ config(materialized='table') }}

with session_geo as (
  select distinct
    coalesce(
        safe_cast((select value.int_value from unnest(event_params) where key = 'ga_session_id') as int64),
        safe_cast((select value.string_value from unnest(event_params) where key = 'ga_session_id') as int64)
    ) as session_id,
    first_value(country) over (partition by (select value.int_value from unnest(event_params) where key = 'ga_session_id') order by event_ts asc) as country,
    first_value(region) over (partition by (select value.int_value from unnest(event_params) where key = 'ga_session_id') order by event_ts asc) as region,
    first_value(city) over (partition by (select value.int_value from unnest(event_params) where key = 'ga_session_id') order by event_ts asc) as city
  from {{ ref('stg_events') }}
  where (select value.int_value from unnest(event_params) where key = 'ga_session_id') is not null
),

sessions as (
  select * from {{ ref('fct_sessions') }}
),

session_commerce as (
  select
    ga_session_id as session_id,
    count(distinct transaction_id) as total_transactions,
    sum(total_revenue) as total_revenue
  from {{ ref('fct_purchases') }}
  group by 1
)

select
  s.session_id,
  s.user_pseudo_id,
  s.event_date,
  s.session_start,
  s.session_end,
  s.session_length,
  s.session_number,
  
  g.country,
  g.region,
  g.city,

  coalesce(c.total_transactions, 0) as total_transactions,
  coalesce(c.total_revenue, 0.0) as total_revenue,
  if(coalesce(c.total_transactions, 0) > 0, 1, 0) as has_transaction

from sessions s
left join session_geo g using (session_id)
left join session_commerce c using (session_id)
