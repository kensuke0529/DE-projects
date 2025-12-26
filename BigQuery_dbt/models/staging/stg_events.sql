{{ config(materialized='view') }}

{% set start_suffix = var('ga4_start_suffix', '20201101') %}
{% set end_suffix   = var('ga4_end_suffix',   '20201107') %}

with base as (
  select
    event_date,
    event_timestamp,
    event_name,
    user_pseudo_id,
    platform,
    device,
    geo,
    traffic_source,
    event_params,
    items
  from {{ source('ga4','events') }}
  where _TABLE_SUFFIX between '{{ start_suffix }}' and '{{ end_suffix }}'
),

renamed as (
  select
    to_hex(sha256(concat(
      user_pseudo_id, '|',
      cast(event_timestamp as string), '|',
      event_name
    ))) as event_id,

    parse_date('%Y%m%d', event_date) as event_date,
    timestamp_micros(event_timestamp) as event_ts,

    user_pseudo_id,
    event_name,
    platform,
    device,
    geo,
    traffic_source,
    event_params,
    items
  from base
)

select * from renamed
