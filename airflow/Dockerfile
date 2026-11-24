# Airflow 3 + Cosmos (runs dbt models as Airflow tasks) + Trino client (for maintenance SQL)
FROM apache/airflow:3.1.3

# Keep Airflow's own version pinned while adding packages (avoids accidental upgrades)
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" \
        "astronomer-cosmos==1.16.0" \
        "trino==0.340.0"

# dbt lives in its OWN virtual environment so its libraries can't clash with Airflow's
RUN python -m venv /opt/airflow/dbt_venv && \
    /opt/airflow/dbt_venv/bin/pip install --no-cache-dir "dbt-core==1.12.7" "dbt-trino==1.12.0"