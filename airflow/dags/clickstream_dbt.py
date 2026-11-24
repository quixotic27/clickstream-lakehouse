"""Every 15 minutes: check bronze is fresh, then rebuild + test silver and gold with dbt.

Cosmos turns every dbt model (and its tests) into its own Airflow task, so the Airflow
graph mirrors the dbt lineage and you can retry a single model instead of the whole project.
"""
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import TestBehavior

DBT_PROJECT_DIR = Path(os.getenv("DBT_PROJECT_DIR", "/opt/airflow/dbt"))
DBT_EXECUTABLE = os.getenv("DBT_EXECUTABLE", "/opt/airflow/dbt_venv/bin/dbt")

profile_config = ProfileConfig(
    profile_name="clickstream",
    target_name="local",
    profiles_yml_filepath=DBT_PROJECT_DIR / "profiles.yml",   # same file you use on your Mac
)

with DAG(
    dag_id="clickstream_dbt",
    description="Freshness check, then dbt build of silver + gold",
    schedule="*/15 * * * *",               # every 15 minutes
    start_date=datetime(2026, 1, 1),
    catchup=False,                         # don't try to "make up" runs for past dates
    max_active_runs=1,                     # never two dbt builds at the same time
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    tags=["clickstream", "dbt"],
) as dag:

    # 1. Stop early if no new data has arrived (the pipeline upstream is broken).
    check_freshness = BashOperator(
        task_id="check_bronze_freshness",
        bash_command=f"{DBT_EXECUTABLE} source freshness "
                     f"--project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}",
    )

    # 2. One Airflow task per dbt model, each followed by that model's tests.
    transform = DbtTaskGroup(
        group_id="dbt_build",
        project_config=ProjectConfig(DBT_PROJECT_DIR),
        profile_config=profile_config,
        execution_config=ExecutionConfig(dbt_executable_path=DBT_EXECUTABLE),
        render_config=RenderConfig(test_behavior=TestBehavior.AFTER_EACH),
    )

    check_freshness >> transform