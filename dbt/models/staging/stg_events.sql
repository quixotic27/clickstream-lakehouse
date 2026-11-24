-- Silver: one row per REAL click.
--   * removes duplicate deliveries (same event_id sent twice by the app)
--   * adds handy derived columns (event_date, revenue, arrival delay)
with source as (

    select * from {{ source('bronze', 'events') }}

),

ranked as (

    select
        *,
        row_number() over (
            partition by event_id                       -- same click...
            order by ingested_at, kafka_partition, kafka_offset   -- ...keep the first copy that arrived
        ) as copy_number
    from source

)

select
    event_id,
    event_type,
    event_ts,
    cast(event_ts as date)                                   as event_date,
    user_id,
    anonymous_id,
    page_type,
    page_url,
    referrer,
    utm_source,
    utm_campaign,
    product_id,
    price,
    quantity,
    case when event_type = 'purchase'
         then price * coalesce(quantity, 1) else 0 end       as revenue,
    search_query,
    order_id,
    device,
    user_agent,
    country,
    schema_version,
    app_version,
    ingested_at,
    date_diff('second', event_ts, ingested_at)               as arrival_delay_seconds
from ranked
where copy_number = 1