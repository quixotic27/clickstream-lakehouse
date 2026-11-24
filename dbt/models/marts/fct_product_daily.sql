-- Gold: product performance per day. "Which products get looked at but never bought?"
with events as (

    select * from {{ ref('int_sessionized_events') }}
    where product_id is not null

),

daily as (

    select
        event_date,
        product_id,
        count_if(event_type = 'product_view')                              as views,
        count(distinct case when event_type = 'product_view' then session_id end) as viewing_sessions,
        count_if(event_type = 'add_to_cart')                               as add_to_carts,
        coalesce(sum(case when event_type = 'purchase' then quantity end), 0) as units_sold,
        sum(revenue)                                                       as revenue
    from events
    group by event_date, product_id

)

select
    *,
    round(cast(add_to_carts as double) / nullif(views, 0), 4)    as view_to_cart_rate
from daily