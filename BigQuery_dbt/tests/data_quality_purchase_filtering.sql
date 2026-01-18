-- Data Quality Test: Track data filtering rates in int_purchase_items
-- This singular test generates a warning if filtering rate exceeds threshold

{% set max_missing_transaction_rate = 0.20 %}  -- 20% threshold

with stats as (
  select
    count(*) as total_rows,
    countif(not is_valid_transaction) as missing_transaction_id,
    countif(not has_items) as missing_items,
    countif(not is_complete) as incomplete_rows
  from {{ ref('int_purchase_items') }}
)

select
  'ALERT: Data filtering exceeds threshold' as issue,
  total_rows,
  missing_transaction_id,
  round(100.0 * missing_transaction_id / nullif(total_rows, 0), 2) as missing_txn_pct,
  missing_items,
  incomplete_rows
from stats
where missing_transaction_id / nullif(total_rows, 0) > {{ max_missing_transaction_rate }}
