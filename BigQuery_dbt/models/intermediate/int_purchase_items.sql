{{ config(materialized='view') }}

with purchase_events as (
  select 
    event_key,
    user_pseudo_id,
    event_date,
    event_ts
  from {{ ref('stg_events') }}
  where event_name = 'purchase'
),

params as (
  select
    event_key,
    transaction_id,
    value_param as event_value, 
    currency,
    ga_session_id                 
  from {{ ref('int_event_param_pivot') }}
),

items as (
  select
    event_key,
    item_id,
    item_name,
    item_brand,
    item_category,
    quantity,
    price,
    item_revenue as ga4_item_revenue
  from {{ ref('stg_items') }}
),

joined as (
  select
    e.event_key as purchase_event_key,
    e.user_pseudo_id,
    e.event_date,
    e.event_ts as purchase_ts,

    p.ga_session_id,
    p.transaction_id,
    p.currency,
    p.event_value,

    i.item_id,
    i.item_name,
    i.item_brand,
    i.item_category,
    i.quantity,
    i.price,

    coalesce(i.ga4_item_revenue, i.price * i.quantity) as item_revenue_calc
  from purchase_events e
  left join params p using (event_key)
  left join items i using (event_key)
),

final as (
  select
    *,

    -- DQ flags
    transaction_id is not null as has_transaction_id,
    item_id is not null or item_name is not null as has_item,

    (
      transaction_id is not null
      and (item_id is not null or item_name is not null)
      and quantity is not null and quantity > 0
      and item_revenue_calc is not null and item_revenue_calc >= 0
    ) as is_valid_purchase_line,

    case
      when transaction_id is null then 'missing_transaction_id'
      when (item_id is null and item_name is null) then 'missing_item'
      when quantity is null then 'missing_quantity'
      when quantity <= 0 then 'nonpositive_quantity'
      when item_revenue_calc is null then 'missing_item_revenue'
      when item_revenue_calc < 0 then 'negative_item_revenue'
      else null
    end as invalid_reason
  from joined
)

select * from final
