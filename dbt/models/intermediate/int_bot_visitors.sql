-- Silver: one row per device (anonymous_id) with a bot verdict.
-- Two independent signals, so a bot that fakes its user agent still gets caught:
--   1. it SAYS it's a bot   (user agent / device label)
--   2. it BEHAVES like one  (lots of events, far faster than a human can click)
with per_visitor as (

    select
        anonymous_id,
        count(*)                                                   as events,
        min(event_ts)                                              as first_seen,
        max(event_ts)                                              as last_seen,
        max(case when device = 'bot'
                   or regexp_like(lower(coalesce(user_agent, '')), 'bot|crawler|spider')
                 then 1 else 0 end)                                as self_declared_bot
    from {{ ref('stg_events') }}
    where anonymous_id is not null
    group by anonymous_id

)

select
    anonymous_id,
    events,
    first_seen,
    last_seen,
    round(events / greatest(date_diff('second', first_seen, last_seen) / 60.0, 1.0), 1)
                                                                   as events_per_minute,
    self_declared_bot = 1                                          as is_declared_bot,
    events >= 30
      and events / greatest(date_diff('second', first_seen, last_seen) / 60.0, 1.0) > 20
                                                                   as is_behavioural_bot,
    self_declared_bot = 1
      or (events >= 30
          and events / greatest(date_diff('second', first_seen, last_seen) / 60.0, 1.0) > 20)
                                                                   as is_bot
from per_visitor