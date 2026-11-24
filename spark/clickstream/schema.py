"""Event contract shared by every streaming job.

The simulator (`simulator/events.py`) produces exactly this shape. Fields added in
schema_version 2 are nullable, so v1 producers stay valid (backward-compatible evolution).
"""
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType, StructField, StructType,
)

EVENT_TYPES = (
    "page_view", "search", "product_view", "add_to_cart",
    "remove_from_cart", "checkout_start", "purchase",
)

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("schema_version", IntegerType()),
    StructField("event_type", StringType()),
    StructField("event_ts", StringType()),        # ISO-8601 from the client; parsed downstream
    StructField("user_id", StringType()),         # null for logged-out traffic
    StructField("anonymous_id", StringType()),    # cookie / device id, always set by real clients
    StructField("page_url", StringType()),
    StructField("page_type", StringType()),
    StructField("referrer", StringType()),
    StructField("utm_source", StringType()),
    StructField("utm_campaign", StringType()),
    StructField("product_id", StringType()),
    StructField("price", DoubleType()),
    StructField("quantity", IntegerType()),
    StructField("search_query", StringType()),
    StructField("order_id", StringType()),
    StructField("device", StringType()),
    StructField("user_agent", StringType()),
    StructField("country", StringType()),
    StructField("app_version", StringType()),     # added in schema_version 2
])

BRONZE_DDL = """
CREATE TABLE IF NOT EXISTS lake.bronze.events (
    event_id        STRING,
    schema_version  INT,
    event_type      STRING,
    event_ts        TIMESTAMP,
    user_id         STRING,
    anonymous_id    STRING,
    page_url        STRING,
    page_type       STRING,
    referrer        STRING,
    utm_source      STRING,
    utm_campaign    STRING,
    product_id      STRING,
    price           DOUBLE,
    quantity        INT,
    search_query    STRING,
    order_id        STRING,
    device          STRING,
    user_agent      STRING,
    country         STRING,
    app_version     STRING,
    kafka_partition INT,
    kafka_offset    BIGINT,
    ingested_at     TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(event_ts))
TBLPROPERTIES (
    'format-version' = '2',
    'write.target-file-size-bytes' = '134217728',
    'write.distribution-mode' = 'hash',
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max' = '50'
)
"""

DLQ_DDL = """
CREATE TABLE IF NOT EXISTS lake.bronze.events_dlq (
    raw_value       STRING,
    error_reason    STRING,
    kafka_partition INT,
    kafka_offset    BIGINT,
    ingested_at     TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(ingested_at))
"""

LIVE_METRICS_DDL = """
CREATE TABLE IF NOT EXISTS lake.gold.live_traffic_1m (
    window_start    TIMESTAMP,
    window_end      TIMESTAMP,
    event_type      STRING,
    events          BIGINT,
    approx_users    BIGINT,
    revenue         DOUBLE
)
USING iceberg
PARTITIONED BY (days(window_start))
"""