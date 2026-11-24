-- Reconciliation test: revenue must be identical at every layer.
-- If a join duplicates or drops rows anywhere, these totals drift apart and this test fails.
-- (A test "fails" when its query returns rows.)
with totals as (
    select
        (select coalesce(sum(revenue), 0) from {{ ref('int_sessionized_events') }}) as events_revenue,
        (select coalesce(sum(revenue), 0) from {{ ref('fct_sessions') }})           as sessions_revenue,
        (select coalesce(sum(revenue), 0) from {{ ref('fct_funnel_daily') }})       as funnel_revenue,
        (select coalesce(sum(revenue), 0) from {{ ref('fct_traffic_daily') }})      as traffic_revenue,
        (select coalesce(sum(revenue), 0) from {{ ref('fct_product_daily') }})      as product_revenue
)
select *
from totals
where abs(events_revenue - sessions_revenue) > 0.01
   or abs(events_revenue - funnel_revenue)   > 0.01
   or abs(events_revenue - traffic_revenue)  > 0.01
   or abs(events_revenue - product_revenue)  > 0.01