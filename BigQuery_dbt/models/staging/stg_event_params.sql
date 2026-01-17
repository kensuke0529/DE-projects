{{ config(materialized='view') }}

{% set start_suffix = var('ga4_start_suffix', '20201101') %}
{% set end_suffix   = var('ga4_end_suffix',   '20201107') %}

select
  e.event_key,
  e.event_date,
  e.event_ts,
  e.user_pseudo_id,
  e.event_name,

  ep.key as param_key,

  ep.value.string_value as param_string_value,
  ep.value.int_value    as param_int_value,
  ep.value.double_value as param_double_value

from {{ ref('stg_events') }} e
cross join unnest(e.event_params) as ep

