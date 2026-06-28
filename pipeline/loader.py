"""
loader.py — Saves the transformed DataFrame to CSV (dev) or Snowflake (production).

Dev workflow:   python run_pipeline.py              → CSV in outputs/
Prod workflow:  python run_pipeline.py --snowflake  → CSV + Snowflake load

Snowflake credentials are read from environment variables (see .env.example).
After the load, dbt handles all further transformations in Snowflake.
"""
import logging
import re
from datetime import datetime
from pathlib import Path

import pandas as pd

from config import (
    OUTPUT_DIR,
    SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD,
    SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA,
    SNOWFLAKE_TABLE, RPI_SNOWFLAKE_TABLE,
)

log = logging.getLogger(__name__)

_VALID_IDENTIFIER = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')


def _validate_identifier(name: str) -> None:
    if not _VALID_IDENTIFIER.match(name):
        raise ValueError(f"Invalid Snowflake table name: {name!r}")


def save_csv(df: pd.DataFrame) -> Path:
    """
    Save the long-format DataFrame to a timestamped CSV.
    Always runs — CSV is the dev output and a useful audit trail in production.
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    ts   = datetime.now().strftime("%Y%m%d_%H%M")
    path = OUTPUT_DIR / f"ukri_innovate_uk_tagged_{ts}.csv"
    df.to_csv(path, index=False)
    log.info(f"CSV saved → {path}  ({len(df):,} rows)")
    return path


def load_to_snowflake(df: pd.DataFrame, overwrite: bool = False) -> None:
    """
    Load the long-format DataFrame into Snowflake (RAW layer).
    dbt models downstream handle staging and mart transformations.

    The table schema matches the DataFrame columns exactly.
    First run creates the table; subsequent runs append by default.

    Args:
        df:        Output of transformer.build_long_dataframe().
        overwrite: If True, TRUNCATE the table before loading.
                   Use this when re-running a full historical backfill.
    """
    try:
        import snowflake.connector
        from snowflake.connector.pandas_tools import write_pandas
    except ImportError:
        raise ImportError(
            "snowflake-connector-python is not installed.\n"
            "Run: pip install 'snowflake-connector-python[pandas]'"
        )

    log.info(
        f"Connecting to Snowflake: "
        f"{SNOWFLAKE_ACCOUNT} / {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE}"
    )
    conn = snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    )

    try:
        cur = conn.cursor()

        _validate_identifier(SNOWFLAKE_TABLE)

        # Create table if it doesn't exist (idempotent)
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {SNOWFLAKE_TABLE} (
                PROJECT_ID      VARCHAR,
                TITLE           VARCHAR,
                STATUS          VARCHAR,
                LEAD_FUNDER     VARCHAR,
                GRANT_CATEGORY  VARCHAR,
                START_DATE      DATE,
                END_DATE        DATE,
                CATEGORY        VARCHAR,
                KEYWORD         VARCHAR,
                FOUND_IN        VARCHAR,
                GTR_URL         VARCHAR,
                INGESTED_AT     TIMESTAMP_TZ
            )
        """)

        if overwrite:
            cur.execute(f"TRUNCATE TABLE {SNOWFLAKE_TABLE}")
            log.info(f"Table {SNOWFLAKE_TABLE} truncated (overwrite=True)")

        # Snowflake write_pandas requires UPPERCASE column names
        df_upload = df.copy()
        df_upload.columns = df_upload.columns.str.upper()

        success, num_chunks, num_rows, _ = write_pandas(
            conn,
            df_upload,
            SNOWFLAKE_TABLE,
            auto_create_table=False,
            overwrite=False,
        )

        if success:
            log.info(f"Snowflake load complete — {num_rows:,} rows in {num_chunks} chunk(s)")
        else:
            log.error("Snowflake write_pandas reported failure — check connector logs")

    finally:
        conn.close()


def load_all_projects_to_snowflake(df: pd.DataFrame, overwrite: bool = False) -> None:
    """
    Load the wide-format all-funders DataFrame into UKRI_ALL_PROJECTS (RAW layer).
    Schema: one row per project — no keyword expansion.
    """
    try:
        import snowflake.connector
        from snowflake.connector.pandas_tools import write_pandas
    except ImportError:
        raise ImportError("pip install 'snowflake-connector-python[pandas]'")

    _validate_identifier(RPI_SNOWFLAKE_TABLE)
    log.info(
        f"Connecting to Snowflake: "
        f"{SNOWFLAKE_ACCOUNT} / {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{RPI_SNOWFLAKE_TABLE}"
    )
    conn = snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    )
    try:
        cur = conn.cursor()
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {RPI_SNOWFLAKE_TABLE} (
                PROJECT_ID      VARCHAR,
                TITLE           VARCHAR,
                STATUS          VARCHAR,
                LEAD_FUNDER     VARCHAR,
                GRANT_CATEGORY  VARCHAR,
                DEPARTMENT      VARCHAR,
                START_DATE      DATE,
                END_DATE        DATE,
                GTR_URL         VARCHAR,
                INGESTED_AT     TIMESTAMP_TZ
            )
        """)
        if overwrite:
            cur.execute(f"TRUNCATE TABLE {RPI_SNOWFLAKE_TABLE}")
            log.info(f"Table {RPI_SNOWFLAKE_TABLE} truncated (overwrite=True)")

        df_upload = df.copy()
        df_upload.columns = df_upload.columns.str.upper()
        success, num_chunks, num_rows, _ = write_pandas(
            conn, df_upload, RPI_SNOWFLAKE_TABLE, auto_create_table=False, overwrite=False,
        )
        if success:
            log.info(f"RPI Snowflake load complete — {num_rows:,} rows in {num_chunks} chunk(s)")
        else:
            log.error("write_pandas reported failure — check connector logs")
    finally:
        conn.close()
