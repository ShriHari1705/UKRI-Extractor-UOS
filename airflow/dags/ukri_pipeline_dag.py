"""
ukri_pipeline_dag.py — Airflow DAG for the UKRI Early Warning System.

Schedule: Every Monday at 06:00 UTC (weekly refresh).

Task flow:
    fetch_and_tag  →  dbt_staging  →  dbt_marts  →  dbt_tests

The Python pipeline handles Extract + initial Load (CSV + Snowflake RAW).
dbt handles all transformations inside Snowflake (staging → marts).

Setup:
    1. Place this file in your Airflow dags/ folder.
    2. Set the Airflow Variable UKRI_PIPELINE_DIR to the repo root path.
    3. Ensure the Airflow worker can reach the Snowflake and UKRI API endpoints.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable

# Path to the cloned repo on the Airflow worker / HPC node
PIPELINE_DIR = Variable.get("UKRI_PIPELINE_DIR", default_var="/opt/ukri-early-warning")
DBT_DIR      = f"{PIPELINE_DIR}/dbt"

default_args = {
    "owner":            "research-it",
    "depends_on_past":  False,
    "email_on_failure": True,
    "email":            ["research-it@sheffield.ac.uk"],
    "retries":          2,
    "retry_delay":      timedelta(minutes=10),
}

with DAG(
    dag_id="ukri_early_warning_pipeline",
    description=(
        "Weekly pipeline: fetch UKRI Innovate UK projects, "
        "tag abstracts, load to Snowflake, run dbt models."
    ),
    schedule_interval="0 6 * * 1",   # Every Monday 06:00 UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["ukri", "research-it", "early-warning"],
) as dag:

    # ── Stage 1: Fetch, tag, load RAW to Snowflake ────────────────────────────
    fetch_and_tag = BashOperator(
        task_id="fetch_and_tag",
        bash_command=(
            f"cd {PIPELINE_DIR} && "
            "source .venv/bin/activate && "
            "python run_pipeline.py --snowflake"
        ),
        execution_timeout=timedelta(hours=6),  # full run can take time (8660 pages)
    )

    # ── Stage 2: dbt staging models (clean + type-cast RAW) ──────────────────
    dbt_staging = BashOperator(
        task_id="dbt_staging",
        bash_command=(
            f"cd {DBT_DIR} && "
            "dbt run --select staging --profiles-dir ."
        ),
    )

    # ── Stage 3: dbt mart models (keyword_tags, early_warning_signal) ─────────
    dbt_marts = BashOperator(
        task_id="dbt_marts",
        bash_command=(
            f"cd {DBT_DIR} && "
            "dbt run --select marts --profiles-dir ."
        ),
    )

    # ── Stage 4: dbt tests ────────────────────────────────────────────────────
    dbt_tests = BashOperator(
        task_id="dbt_tests",
        bash_command=(
            f"cd {DBT_DIR} && "
            "dbt test --profiles-dir ."
        ),
    )

    fetch_and_tag >> dbt_staging >> dbt_marts >> dbt_tests
