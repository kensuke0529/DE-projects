{{ config(materialized='view') }}

with pivoted as (
  select
    event_key,
    event_date,
    event_ts,
    user_pseudo_id,

    max(if(param_key='ga_session_id', param_int_value, null)) as ga_session_id,
    max(if(param_key='ga_session_number', param_int_value, null)) as ga_session_number,
    max(if(param_key='page_location', param_string_value, null)) as page_location,
    max(if(param_key='page_referrer', param_string_value, null)) as page_referrer,
    max(if(param_key='transaction_id', param_string_value, null)) as transaction_id,
    max(if(param_key='page_title', param_string_value, null)) as page_title,
    max(if(param_key='currency', param_string_value, null)) as currency,
    max(if(param_key='value', param_double_value, null)) as value_param

  from {{ ref('stg_event_params') }}
  group by 1,2,3,4
)

select * from pivoted
