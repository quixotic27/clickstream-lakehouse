-- Data quality: in the last hour, fewer than 2% of incoming messages may fail the event contract.
-- A spike usually means an app release broke the tracking SDK.
-- severity=warn: report it loudly, but don't block the gold tables from building.
{{ config(severity='warn') }}

with last_hour as (
    select
        (select count(*) from {{ source('bronze', 'events') }}
          where ingested_at > current_timestamp - interval '1' hour)      as good_messages,
        (select count(*) from {{ source('bronze', 'events_dlq') }}
          where ingested_at > current_timestamp - interval '1' hour)      as bad_messages
)
select
    good_messages,
    bad_messages,
    round(cast(bad_messages as double) / nullif(good_messages + bad_messages, 0), 4) as dlq_rate
from last_hour
where cast(bad_messages as double) / nullif(good_messages + bad_messages, 0) > 0.02