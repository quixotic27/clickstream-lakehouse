-- Data quality: a device id (anonymous_id) is ONE physical device, so it can't be both
-- "mobile" and "desktop". If this fails, the tracking SDK is mixing up ids.
-- (This is the exact bug we found and fixed in the simulator in Step 1.)
select
    anonymous_id,
    count(distinct device) as device_types
from {{ ref('stg_events') }}
where anonymous_id is not null
group by anonymous_id
having count(distinct device) > 1