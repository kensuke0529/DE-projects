{{ config(materialized='view') }}

{% set start_date = var('start_date', none) %}
{% set end_date   = var('end_date', none) %}

with base as (
  select
    parse_date('%Y%m%d', event_date) as event_date,
    timestamp_micros(event_timestamp) as event_ts,

    user_pseudo_id,
    event_name,
    platform,

    device.category as device_category,
    device.operating_system as operating_system,
    device.web_info.browser as browser,

    geo.country as country,
    geo.region as region,
    geo.city as city,

    event_params,
    items,
    event_bundle_sequence_id

  from {{ source('ga4','events') }}

  {% if start_date and end_date %}
  where event_date between '{{ start_date }}' and '{{ end_date }}'
  {% endif %}
),

final as (
  select
    *,
    to_hex(sha256(concat(
      user_pseudo_id, '|',
      cast(event_ts as string), '|',
      event_name, '|',
      cast(coalesce(event_bundle_sequence_id, 0) as string)
    ))) as event_key
  from base
)

select * from final
