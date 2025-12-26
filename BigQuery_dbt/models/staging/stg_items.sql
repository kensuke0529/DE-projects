{{ config(materialized='view') }}

{% set start_suffix = var('ga4_start_suffix', '20201101') %}
{% set end_suffix   = var('ga4_end_suffix',   '20201107') %}

with base as (
  select
    event_date,
    event_timestamp,
    event_name,
    user_pseudo_id,
    items
  from {{ source('ga4', 'events') }}
  where _TABLE_SUFFIX between '{{ start_suffix }}' and '{{ end_suffix }}'
),

exploded as (
  select
    to_hex(sha256(concat(
      b.user_pseudo_id, '|',
      cast(b.event_timestamp as string), '|',
      b.event_name
    ))) as event_id,

    safe.parse_date('%Y%m%d', b.event_date) as event_date,
    timestamp_micros(b.event_timestamp) as event_ts,

    b.user_pseudo_id,
    b.event_name,

    i.item_id,
    i.item_name,
    i.item_brand,
    i.item_variant,
    i.item_category,
    i.item_category2,
    i.item_category3,
    i.item_category4,
    i.item_category5,

    i.price,
    i.quantity
  from base b
  cross join unnest(b.items) as i
)

select * from exploded
