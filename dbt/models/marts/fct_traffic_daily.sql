-- Gold: marketing performance per day, channel and campaign.
-- "Did the Diwali sale ad on Meta actually bring buyers, or just clicks?"
with sessions as (

    select * from {{ ref('fct_sessions') }}

),

daily as (

    select
        session_date,
        channel,
        coalesce(campaign, '(none)')            as campaign,
        count(*)                                as sessions,
        count(distinct anonymous_id)            as visitors,
        count_if(is_bounce)                     as bounced_sessions,
        count_if(converted)                     as converted_sessions,
        sum(orders)                             as orders,
        sum(revenue)                            as revenue
    from sessions
    group by session_date, channel, coalesce(campaign, '(none)')

)

select
    *,
    round(cast(bounced_sessions   as double) / nullif(sessions, 0), 4)  as bounce_rate,
    round(cast(converted_sessions as double) / nullif(sessions, 0), 4)  as conversion_rate,
    round(revenue / nullif(sessions, 0), 2)                             as revenue_per_session
from daily