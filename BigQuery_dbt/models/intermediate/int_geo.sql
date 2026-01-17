{{ config(materialized='view') }}

select
  event_key,
  country,
  region,
  city
from {{ ref('stg_events') }}
