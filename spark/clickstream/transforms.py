"""Pure DataFrame transforms. No I/O here, so every function is unit-testable on batch data
and reused unchanged by the streaming jobs."""
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from clickstream.schema import EVENT_SCHEMA, EVENT_TYPES

# Events claiming to be more than this far in the future are clock-skewed clients.
MAX_FUTURE_SKEW = "10 minutes"


def parse_kafka(raw: DataFrame) -> DataFrame:
    """Kafka records -> parsed struct + metadata. Keeps the raw string for the DLQ."""
    return (
        raw.select(
            F.col("value").cast("string").alias("raw_value"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
        )
        .withColumn("e", F.from_json("raw_value", EVENT_SCHEMA))
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("event_ts_parsed", F.to_timestamp(F.col("e.event_ts")))
    )


def error_reason(df: DataFrame) -> Column:
    """First failing contract rule, or null when the event is valid. Order matters: the
    cheapest, most fundamental checks come first."""
    e = lambda c: F.col(f"e.{c}")  # noqa: E731
    return (
        F.when(F.col("e").isNull() | e("event_id").isNull(), F.lit("malformed_json"))
        .when(e("user_id").isNull() & e("anonymous_id").isNull(), F.lit("missing_identity"))
        .when(F.col("event_ts_parsed").isNull(), F.lit("bad_timestamp"))
        .when(~e("event_type").isin(*EVENT_TYPES), F.lit("unknown_event_type"))
        .when(
            F.col("event_ts_parsed") > F.col("ingested_at") + F.expr(f"INTERVAL {MAX_FUTURE_SKEW}"),
            F.lit("future_timestamp"),
        )
        .when((e("event_type") == "purchase") & (e("order_id").isNull() | (e("price") <= 0)),
              F.lit("invalid_purchase"))
    )


def with_validation(parsed: DataFrame) -> DataFrame:
    return parsed.withColumn("error_reason", error_reason(parsed))


def to_bronze(validated: DataFrame) -> DataFrame:
    """Valid events flattened to the bronze schema. Duplicates are KEPT in bronze on purpose:
    bronze is the replayable source of truth; dedupe happens in silver.

    Column ORDER must match the table: Iceberg checks it on write. So we walk EVENT_SCHEMA in
    order and swap in the parsed timestamp where event_ts sits.
    """
    cols = [
        F.col("event_ts_parsed").alias("event_ts") if f.name == "event_ts"
        else F.col(f"e.{f.name}").alias(f.name)
        for f in EVENT_SCHEMA.fields
    ]
    return (
        validated.where(F.col("error_reason").isNull())
        .select(*cols, "kafka_partition", "kafka_offset", "ingested_at")
    )


def to_dlq(validated: DataFrame) -> DataFrame:
    return validated.where(F.col("error_reason").isNotNull()).select(
        "raw_value", "error_reason", "kafka_partition", "kafka_offset", "ingested_at"
    )


def live_traffic(bronze_like: DataFrame, watermark: str = "2 minutes",
                 window: str = "1 minute") -> DataFrame:
    """Speed-layer metrics (used in Step 5): per-minute counts, approx unique users and revenue.

    - dropDuplicatesWithinWatermark removes producer retries without unbounded state.
    - Events later than the watermark are dropped HERE but still land in bronze, so the
      batch layer (dbt) produces the corrected numbers. Classic speed/batch trade-off.
    """
    identity = F.coalesce("user_id", "anonymous_id")
    revenue = F.when(F.col("event_type") == "purchase",
                     F.col("price") * F.coalesce("quantity", F.lit(1))).otherwise(0.0)
    return (
        bronze_like.withWatermark("event_ts", watermark)
        .dropDuplicatesWithinWatermark(["event_id"])
        .groupBy(F.window("event_ts", window), "event_type")
        .agg(
            F.count("*").alias("events"),
            F.approx_count_distinct(identity).alias("approx_users"),
            F.round(F.sum(revenue), 2).alias("revenue"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "event_type", "events", "approx_users", "revenue",
        )
    )