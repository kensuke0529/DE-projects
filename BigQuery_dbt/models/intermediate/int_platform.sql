{{ config(materialized='view') }}

select 
  event_key,
  platform,
  device_category,
  operating_system,
  browser
from {{ ref('stg_events') }}
