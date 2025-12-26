{{ config(materialized='table') }}

with page_views as (
    select
        event_date,
        user_pseudo_id
    from {{ ref('int_page_views') }}
),

final as (
    select
        event_date,
        count(*) as total_page_views,
        count(distinct user_pseudo_id) as unique_users
    from page_views
    group by event_date
)

select * from final