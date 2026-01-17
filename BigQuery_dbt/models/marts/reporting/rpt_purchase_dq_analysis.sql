{{ config(materialized='table') }}

select
    transaction_id,
    user_pseudo_id,
    event_date,
    purchase_event_key,
    invalid_reason,
    item_name,
    item_revenue_calc as estimated_revenue,
    quantity
from {{ ref('int_purchase_items') }}
where not is_valid_purchase_line
order by event_date desc
