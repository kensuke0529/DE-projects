
select
    event_date,
    event_name,
    count(*) as total_events,
    count(distinct transaction_id) as distinct_transactions,
    count(case when transaction_id is null then 1 end) as null_transactions,
    count(case when transaction_id = '' then 1 end) as empty_transactions,
    sum(value) as total_revenue
from {{ ref('stg_event_param_pivot') }}
where event_name = 'purchase'
  and event_date between '2020-11-01' and '2020-11-10'
group by 1, 2
order by 1
