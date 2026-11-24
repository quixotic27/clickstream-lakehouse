"""Session + source helpers for the streaming jobs. All knobs come from env vars so the same
code runs locally, in CI and on a cluster."""
import os

from pyspark.sql import DataFrame, SparkSession

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "redpanda:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "clickstream.events")
CHECKPOINT_ROOT = os.getenv("CHECKPOINT_ROOT", "/opt/checkpoints")
TRIGGER = os.getenv("TRIGGER_INTERVAL", "30 seconds")
# Back-pressure: cap records per micro-batch so a backlog can't OOM the driver.
MAX_OFFSETS = os.getenv("MAX_OFFSETS_PER_TRIGGER", "200000")


def spark(app: str) -> SparkSession:
    return SparkSession.builder.appName(app).getOrCreate()


def kafka_stream(s: SparkSession) -> DataFrame:
    return (
        s.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", MAX_OFFSETS)
        .option("failOnDataLoss", "false")
        .load()
    )


def ensure_namespaces(s: SparkSession, *ddls: str) -> None:
    for ns in ("bronze", "silver", "gold"):
        s.sql(f"CREATE NAMESPACE IF NOT EXISTS lake.{ns}")
    for ddl in ddls:
        s.sql(ddl)


def checkpoint(name: str) -> str:
    return f"{CHECKPOINT_ROOT}/{name}"