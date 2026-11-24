-- Gold: one row per session (visit). The table most product analytics is built on.
with events as (

    select * from {{ ref('int_sessionized_events') }}

)

select
    session_id,
    anonymous_id,
    max(user_id)                                                  as user_id,
    min(event_date)                                               as session_date,
    min(event_ts)                                                 as session_start,
    max(event_ts)                                                 as session_end,
    date_diff('second', min(event_ts), max(event_ts))             as duration_seconds,

    -- where they came from / what they used (taken from the FIRST click of the visit)
    coalesce(min_by(utm_source, event_seq_in_session), 'direct')  as channel,
    min_by(utm_campaign, event_seq_in_session)                    as campaign,
    min_by(device, event_seq_in_session)                          as device,
    min_by(country, event_seq_in_session)                         as country,
    min_by(page_url, event_seq_in_session)                        as landing_page,
    max_by(page_url, event_seq_in_session)                        as exit_page,

    -- what they did
    count(*)                                                      as events,
    count_if(event_type = 'product_view')                         as product_views,
    count_if(event_type = 'search')                               as searches,
    count(*) = 1                                                  as is_bounce,
    count_if(event_type = 'product_view') > 0                     as viewed_product,
    count_if(event_type = 'add_to_cart') > 0                      as added_to_cart,
    count_if(event_type = 'checkout_start') > 0                   as started_checkout,
    count_if(event_type = 'purchase') > 0                         as converted,
    count(distinct order_id)                                      as orders,
    sum(revenue)                                                  as revenue
from events
group by session_id, anonymous_id