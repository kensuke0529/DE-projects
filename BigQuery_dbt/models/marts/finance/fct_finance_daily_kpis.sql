{{ config(
    materialized = 'table'
) }}

with events as (
    select *
    from {{ ref('stg_event_param_pivot') }}
),

-- 1. Calculate Daily Sessions
daily_sessions as (
    select
        event_date,
        count(distinct concat(user_pseudo_id, cast(ga_session_id as string))) as sessions
    from events
    where ga_session_id is not null
    group by 1
),

-- 2. Calculate Daily Orders & Revenue
daily_orders as (
    select
        event_date,
        count(distinct coalesce(transaction_id, event_id)) as orders,
        sum(value) as revenue
    from events
    where event_name = 'purchase'
    group by 1
),

-- 3. Join & Calculate Ratios
final as (
    select
        coalesce(s.event_date, o.event_date) as date,
        coalesce(s.sessions, 0) as sessions,
        coalesce(o.orders, 0) as orders,
        coalesce(o.revenue, 0) as revenue,
        
        -- AOV
        case 
            when coalesce(o.orders, 0) > 0 
            then coalesce(o.revenue, 0) / o.orders 
            else 0 
        end as aov,

        -- Conversion Rate
        case 
            when coalesce(s.sessions, 0) > 0 
            then coalesce(o.orders, 0) / s.sessions 
            else 0 
        end as conversion_rate

    from daily_sessions s
    full outer join daily_orders o
        using (event_date)
)

select * from final
order by date
