{{ config(materialized='table') }}

with purchase_items as (
  select
    product_key,
    item_id,
    item_name,
    item_brand,
    item_category
  from {{ ref('int_purchase_items') }}
  where has_items  -- Only include records with valid product data
)

select distinct
  product_key,
  item_id,
  item_name,
  item_brand,
  item_category
from purchase_items
where product_key is not null
