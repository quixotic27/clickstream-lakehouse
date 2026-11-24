-- Gold: the conversion funnel per day and device.
-- "Of all visits, how many viewed a product -> added to cart -> started checkout -> bought?"
with sessions as (

    select * from {{ ref('fct_sessions') }}

),

counts as (

    select
        session_date,
        device,
        count(*)                          as sessions,
        count_if(viewed_product)          as sessions_viewed_product,
        count_if(added_to_cart)           as sessions_added_to_cart,
        count_if(started_checkout)        as sessions_started_checkout,
        count_if(converted)               as sessions_converted,
        sum(revenue)                      as revenue
    from sessions
    group by session_date, device

)

select
    *,
    -- cast to double: in Trino, integer / integer is INTEGER division (1 / 4 = 0)
    round(cast(sessions_viewed_product   as double) / nullif(sessions, 0), 4)                  as view_rate,
    round(cast(sessions_added_to_cart    as double) / nullif(sessions_viewed_product, 0), 4)   as view_to_cart_rate,
    round(cast(sessions_started_checkout as double) / nullif(sessions_added_to_cart, 0), 4)    as cart_to_checkout_rate,
    round(cast(sessions_converted        as double) / nullif(sessions_started_checkout, 0), 4) as checkout_to_purchase_rate,
    round(cast(sessions_converted        as double) / nullif(sessions, 0), 4)                  as conversion_rate
from counts