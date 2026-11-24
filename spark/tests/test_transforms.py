import json

import pytest
from pyspark.sql import SparkSession

from clickstream.transforms import parse_kafka, to_bronze, to_dlq, with_validation


@pytest.fixture(scope="session")
def spark():
    s = (SparkSession.builder.master("local[1]").appName("tests")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.ui.enabled", "false").getOrCreate())
    yield s
    s.stop()


def kafka_df(spark, payloads):
    rows = [(p.encode() if p is not None else None, 0, i) for i, p in enumerate(payloads)]
    return spark.createDataFrame(rows, "value binary, partition int, offset long")


def ev(**overrides):
    base = {"event_id": "e1", "schema_version": 1, "event_type": "page_view",
            "event_ts": "2026-11-20T10:00:00.000Z", "user_id": None, "anonymous_id": "anon-1",
            "device": "mobile"}
    base.update(overrides)
    return json.dumps(base)


def test_valid_and_invalid_events_are_split(spark):
    payloads = [
        ev(),                                                  # valid
        ev(event_id="e2", app_version="5.14.0", schema_version=2),  # valid, v2 field
        '{"event_id": "e3", "event_ty',                        # truncated JSON
        ev(event_id="e4", anonymous_id=None),                  # no identity
        ev(event_id="e5", event_type="pageview"),              # unknown type
        ev(event_id="e6", event_ts="yesterday-ish"),           # bad timestamp
        ev(event_id="e7", event_type="purchase", price=100.0), # purchase without order_id
    ]
    validated = with_validation(parse_kafka(kafka_df(spark, payloads)))
    bronze = {r.event_id: r for r in to_bronze(validated).collect()}
    dlq = {r.kafka_offset: r.error_reason for r in to_dlq(validated).collect()}

    assert set(bronze) == {"e1", "e2"}
    assert bronze["e2"].app_version == "5.14.0"
    assert bronze["e1"].event_ts is not None
    assert dlq == {2: "malformed_json", 3: "missing_identity", 4: "unknown_event_type",
                   5: "bad_timestamp", 6: "invalid_purchase"}


def test_duplicates_are_kept_in_bronze(spark):
    validated = with_validation(parse_kafka(kafka_df(spark, [ev(), ev()])))
    assert to_bronze(validated).count() == 2


def test_future_timestamps_are_rejected(spark):
    validated = with_validation(parse_kafka(kafka_df(spark, [ev(event_ts="2099-01-01T00:00:00Z")])))
    assert [r.error_reason for r in to_dlq(validated).collect()] == ["future_timestamp"]


def test_bronze_columns_match_table_order(spark):
    import re
    from clickstream.schema import BRONZE_DDL
    ddl_cols = re.findall(r"^\s{4}(\w+)\s+[A-Z]", BRONZE_DDL, re.M)
    validated = with_validation(parse_kafka(kafka_df(spark, [ev()])))
    assert to_bronze(validated).columns == ddl_cols