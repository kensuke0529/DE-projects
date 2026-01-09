{{ config(materialized='view') }}

{% set start_suffix = var('ga4_start_suffix', '20201101') %}
{% set end_suffix   = var('ga4_end_suffix',   '20201107') %}

with base as (
  select
    event_date,
    event_timestamp,
    event_name,
    user_pseudo_id,
    event_params
  from {{ source('ga4', 'events') }}
  where _TABLE_SUFFIX between '{{ start_suffix }}' and '{{ end_suffix }}'
),

exploded as (
  select
    -- must match how you generate event_id in stg_ga4__events
    to_hex(sha256(concat(
      b.user_pseudo_id, '|',
      cast(b.event_timestamp as string), '|',
      b.event_name
    ))) as event_id,

    safe.parse_date('%Y%m%d', b.event_date) as event_date,
    timestamp_micros(b.event_timestamp) as event_ts,

    b.user_pseudo_id,
    b.event_name,

    ep.key as param_key,
    ep.value.string_value as string_value,
    ep.value.int_value    as int_value,
    ep.value.float_value  as float_value,
    ep.value.double_value as double_value

  from base b
  cross join unnest(b.event_params) as ep
)

select * from exploded
