-- Test query to check if source data exists and has purchase events
SELECT 
  event_name,
  COUNT(*) as event_count,
  MIN(event_date) as min_date,
  MAX(event_date) as max_date
FROM {{ source('ga4', 'events') }}
WHERE _TABLE_SUFFIX BETWEEN '{{ var("ga4_start_suffix") }}' AND '{{ var("ga4_end_suffix") }}'
GROUP BY event_name
ORDER BY event_count DESC
LIMIT 20
