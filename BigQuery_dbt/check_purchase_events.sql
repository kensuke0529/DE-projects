-- Check for purchase events in the source data
SELECT 
  event_name,
  COUNT(*) as count,
  MIN(event_date) as min_date,
  MAX(event_date) as max_date
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
WHERE _TABLE_SUFFIX BETWEEN '20201101' AND '20210131'
  AND event_name = 'purchase'
GROUP BY event_name
