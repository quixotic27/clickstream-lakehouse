-- Data quality: no event may claim to happen more than 10 minutes after it reached us.
-- Spark already routes these to the DLQ; this test proves nothing slipped through.
select event_id, event_ts, ingested_at
from {{ ref('stg_events') }}
where event_ts > ingested_at + interval '10' minute