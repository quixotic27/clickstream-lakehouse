-- Silver: every human click, labelled with the session (visit) it belongs to.
--
-- Rule: a device's clicks belong to the same session until there is a gap of more than
-- 30 minutes with no activity (the same default Google Analytics uses). The next click after
-- such a gap starts a new session.
--
-- Technique ("gaps and islands"):
--   1. lag()  -> time since this device's previous click
--   2. flag   -> 1 if that gap > 30 min (or it's the device's first click), else 0
--   3. running sum of the flags -> session number 1, 2, 3 ... per device
{% set timeout_minutes = var('session_timeout_minutes', 30) %}

with human_events as (

    select e.*
    from {{ ref('stg_events') }} as e
    left join {{ ref('int_bot_visitors') }} as b
        on e.anonymous_id = b.anonymous_id
    where e.anonymous_id is not null
      and coalesce(b.is_bot, false) = false          -- bots don't have "sessions"

),

with_previous as (

    select
        *,
        lag(event_ts) over (
            partition by anonymous_id
            order by event_ts, event_id
        ) as previous_event_ts
    from human_events

),

flagged as (

    select
        *,
        case
            when previous_event_ts is null then 1
            when date_diff('second', previous_event_ts, event_ts) > {{ timeout_minutes }} * 60 then 1
            else 0
        end as is_new_session
    from with_previous

),

numbered as (

    select
        *,
        sum(is_new_session) over (
            partition by anonymous_id
            order by event_ts, event_id
            rows between unbounded preceding and current row
        ) as session_number
    from flagged

)

select
    anonymous_id || '-' || cast(session_number as varchar)        as session_id,
    session_number,
    row_number() over (
        partition by anonymous_id, session_number
        order by event_ts, event_id
    )                                                             as event_seq_in_session,
    date_diff('second', previous_event_ts, event_ts)              as seconds_since_previous_event,
    event_id,
    event_type,
    event_ts,
    event_date,
    user_id,
    anonymous_id,
    page_type,
    page_url,
    utm_source,
    utm_campaign,
    product_id,
    price,
    quantity,
    revenue,
    search_query,
    order_id,
    device,
    country
from numbered