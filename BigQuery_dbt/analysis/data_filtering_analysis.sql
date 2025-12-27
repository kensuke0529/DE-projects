-- Analysis: Data Filtering Due to JOIN/Logic Mismatches
-- This query helps identify how much data is lost at each transformation step

-- =====================================================
-- 1. PURCHASE EVENTS vs PURCHASE ITEMS MISMATCH
-- =====================================================
-- In int_purchase_items.sql, we do INNER JOINs which can filter data

-- Count purchase events in stg_events
with purchase_events as (
  select count(*) as total_purchase_events
  from `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  where _TABLE_SUFFIX between '20201101' and '20210131'
    and event_name = 'purchase'
),

-- Count purchase events with matching params (transaction_id not null)
purchase_with_params as (
  select count(distinct e.event_id) as events_with_transaction_id
  from `kensuke_ecommerce_dev.stg_events` e
  join `kensuke_ecommerce_dev.stg_event_param_pivot` p using (event_id)
  where e.event_name = 'purchase'
    and p.transaction_id is not null
),

-- Count purchase events with items
purchase_with_items as (
  select count(distinct e.event_id) as events_with_items
  from `kensuke_ecommerce_dev.stg_events` e
  join `kensuke_ecommerce_dev.stg_items` i using (event_id)
  where e.event_name = 'purchase'
),

-- =====================================================
-- 2. SESSION DATA MISMATCH (int_sessions.sql)
-- =====================================================
-- Events filtered where ga_session_id is null

events_without_session_id as (
  select count(*) as events_missing_session_id
  from `kensuke_ecommerce_dev.stg_events` e
  left join `kensuke_ecommerce_dev.stg_event_param_pivot` p using (event_id)
  where p.ga_session_id is null
),

total_events as (
  select count(*) as total_stg_events
  from `kensuke_ecommerce_dev.stg_events`
),

-- =====================================================
-- 3. FINAL INT_PURCHASE_ITEMS ROW COUNT
-- =====================================================
final_purchase_items as (
  select count(*) as final_item_rows
  from `kensuke_ecommerce_dev.int_purchase_items`
)

-- =====================================================
-- SUMMARY REPORT
-- =====================================================
select
  'Total Purchase Events (stg_events)' as metric,
  (select count(*) from `kensuke_ecommerce_dev.stg_events` where event_name = 'purchase') as count
union all
select
  'Purchase Events with Transaction ID',
  (select events_with_transaction_id from purchase_with_params)
union all
select
  'Purchase Events with Items',
  (select events_with_items from purchase_with_items)
union all
select
  'Final Purchase Item Rows (int_purchase_items)',
  (select final_item_rows from final_purchase_items)
union all
select
  '------- SESSION ANALYSIS -------',
  null
union all
select
  'Total Events (stg_events)',
  (select total_stg_events from total_events)
union all
select
  'Events Missing ga_session_id (filtered out)',
  (select events_missing_session_id from events_without_session_id)
union all
select
  'Events with Session ID (included in int_sessions)',
  (select total_stg_events from total_events) - (select events_missing_session_id from events_without_session_id)
