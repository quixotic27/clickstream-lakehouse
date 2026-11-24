"""Kafka -> per-minute live traffic metrics (speed layer).

Reads Kafka directly instead of tailing bronze, to keep latency at roughly the trigger interval
plus the watermark. Output is append-only: a window is written once, after the watermark passes it.
"""
import os

from clickstream import io
from clickstream.schema import LIVE_METRICS_DDL
from clickstream.transforms import live_traffic, parse_kafka, to_bronze, with_validation

WATERMARK = os.getenv("LIVE_WATERMARK", "2 minutes")


def main() -> None:
    s = io.spark("stream-live-metrics")
    io.ensure_namespaces(s, LIVE_METRICS_DDL)

    events = to_bronze(with_validation(parse_kafka(io.kafka_stream(s))))

    (live_traffic(events, watermark=WATERMARK).writeStream.format("iceberg")
        .queryName("live_traffic_1m")
        .outputMode("append")
        .trigger(processingTime=io.TRIGGER)
        .option("checkpointLocation", io.checkpoint("live_traffic_1m"))
        .toTable("lake.gold.live_traffic_1m")
        .awaitTermination())


if __name__ == "__main__":
    main()