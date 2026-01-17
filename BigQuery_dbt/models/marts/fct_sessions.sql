{{ config(materialized='view') }}

with sessions as (
  select
    user_pseudo_id,
    session_date as event_date,
    session_start_ts as session_start,
    session_end_ts as session_end,
    session_duration_seconds as session_length,
    session_number,
    ga_session_id as session_id
  from {{ ref('int_sessions') }}
)

select
  user_pseudo_id,
  event_date,
  session_start,
  session_end,
  session_length,
  session_number,
  session_id
from sessions
where 
    session_length > 0
    and session_number > 0
    and session_start is not null
    and session_end is not null
    and session_length is not null
    and session_number is not null
