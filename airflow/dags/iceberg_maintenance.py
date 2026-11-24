"""Nightly Iceberg housekeeping.

Streaming commits every 30 seconds create thousands of tiny Parquet files and snapshots.
Small files make every query slower (more files to open), and old snapshots keep deleted
data on disk. This DAG fixes both, one table at a time:
  1. optimize            -> compact small files into ~128 MB files
  2. expire_snapshots    -> forget snapshots older than 7 days (limits time travel to 7 days)
  3. remove_orphan_files -> delete files no snapshot points to any more
"""
import os
from datetime import datetime, timedelta

import trino
from airflow.sdk import DAG, task

TRINO_HOST = os.getenv("TRINO_HOST", "trino")

TABLES = [
    "lake.bronze.events",
    "lake.bronze.events_dlq",
    "lake.gold.live_traffic_1m",
]


def run_sql(statement: str) -> None:
    conn = trino.dbapi.connect(host=TRINO_HOST, port=8080, user="airflow", catalog="lake")
    cur = conn.cursor()
    print(f"Running: {statement}")
    cur.execute(statement)
    print(cur.fetchall())


with DAG(
    dag_id="iceberg_maintenance",
    description="Compact small files, expire old snapshots, remove orphan files",
    schedule="0 2 * * *",                  # every night at 02:00 UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=5)},  # streaming commits can conflict; retry
    tags=["clickstream", "iceberg", "maintenance"],
) as dag:

    @task
    def compact(table: str) -> str:
        run_sql(f"ALTER TABLE {table} EXECUTE optimize(file_size_threshold => '128MB')")
        return table

    @task
    def expire_snapshots(table: str) -> str:
        run_sql(f"ALTER TABLE {table} EXECUTE expire_snapshots(retention_threshold => '7d')")
        return table

    @task
    def remove_orphans(table: str) -> None:
        run_sql(f"ALTER TABLE {table} EXECUTE remove_orphan_files(retention_threshold => '7d')")

    # Same 3-step chain for every table, mapped dynamically (one task instance per table).
    remove_orphans.expand(table=expire_snapshots.expand(table=compact.expand(table=TABLES)))