"""Kafka -> Iceberg bronze (+ DLQ).

Two independent streaming queries over the same topic, each with its own checkpoint.
Spark checkpoints the Kafka offsets and Iceberg commits atomically per micro-batch, so each
sink is exactly-once end to end. (A single foreachBatch writing both tables would NOT be,
because the two Iceberg commits can't be made atomic together.)
"""
from clickstream import io
from clickstream.schema import BRONZE_DDL, DLQ_DDL
from clickstream.transforms import parse_kafka, to_bronze, to_dlq, with_validation


def main() -> None:
    s = io.spark("stream-bronze")
    io.ensure_namespaces(s, BRONZE_DDL, DLQ_DDL)

    validated = with_validation(parse_kafka(io.kafka_stream(s)))

    (to_bronze(validated).writeStream.format("iceberg")
        .queryName("bronze_events")
        .outputMode("append")
        .trigger(processingTime=io.TRIGGER)
        .option("checkpointLocation", io.checkpoint("bronze_events"))
        .option("fanout-enabled", "true")
        .toTable("lake.bronze.events"))

    (to_dlq(validated).writeStream.format("iceberg")
        .queryName("bronze_dlq")
        .outputMode("append")
        .trigger(processingTime=io.TRIGGER)
        .option("checkpointLocation", io.checkpoint("bronze_dlq"))
        .toTable("lake.bronze.events_dlq"))

    s.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()