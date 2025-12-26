{{ config(materialized='table') }}

with purchase_items as (
  select
    event_date,
    product_key,
    item_name,
    item_brand,
    item_category,
    transaction_id,
    quantity,
    item_revenue,
    unit_price
  from {{ ref('int_purchase_items') }}
)

select
  event_date,
  product_key,
  item_name,
  item_brand,
  item_category,
  count(distinct transaction_id) as transactions,
  sum(quantity) as units_sold,
  sum(item_revenue) as total_revenue,
  avg(unit_price) as avg_unit_price
from purchase_items
group by 1, 2, 3, 4, 5
