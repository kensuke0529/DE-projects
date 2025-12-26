{{ config(materialized='view') }}

with p as (
  select *
  from {{ ref('stg_event_params') }}
),

pivoted as (
  select
    event_id,
    user_pseudo_id,
    event_name,
    event_date,
    event_ts,

    max(if(param_key = 'ga_session_id', int_value, null)) as ga_session_id,
    max(if(param_key = 'ga_session_number', int_value, null)) as ga_session_number,
    max(if(param_key = 'page_location', string_value, null)) as page_location,
    max(if(param_key = 'page_referrer', string_value, null)) as page_referrer,

    max(if(param_key = 'transaction_id', string_value, null)) as transaction_id,
    max(if(param_key = 'value', coalesce(double_value, float_value), null)) as value,
    max(if(param_key = 'currency', string_value, null)) as currency

  from p
  group by event_id, user_pseudo_id, event_name, event_date, event_ts
)

select * from pivoted
