{{ config(materialized='table') }}

with items as (
  select
    transaction_id,
    item_id,
    item_name,
    item_brand,
    item_category,
    price
  from {{ ref('int_purchase_items') }}
  where has_transaction_id and has_item
)

select * 
from items 