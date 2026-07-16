"""
loader.py — Saves the transformed DataFrame to CSV (dev) or MotherDuck (production).

Dev workflow:   python run_pipeline.py               → CSV in outputs/
Prod workflow:  python run_pipeline.py --motherduck  → CSV + MotherDuck load

MotherDuck credentials are read from environment variables (see .env.example).
After the load, dbt handles all further transformations in MotherDuck.
"""
import logging
import re
from datetime import datetime
from pathlib import Path

import pandas as pd

from config import (
    OUTPUT_DIR,
    MOTHERDUCK_TOKEN, MOTHERDUCK_DATABASE, MOTHERDUCK_SCHEMA,
    MOTHERDUCK_TABLE, RPI_MOTHERDUCK_TABLE,
)

log = logging.getLogger(__name__)

_VALID_IDENTIFIER = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')


def _validate_identifier(name: str) -> None:
    if not _VALID_IDENTIFIER.match(name):
        raise ValueError(f"Invalid MotherDuck table name: {name!r}")


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


def _connect():
    import duckdb
    return duckdb.connect(f"md:{MOTHERDUCK_DATABASE}?motherduck_token={MOTHERDUCK_TOKEN}")


def load_to_motherduck(df: pd.DataFrame, overwrite: bool = False) -> None:
    """
    Load the long-format DataFrame into MotherDuck (RAW layer).
    dbt models downstream handle staging and mart transformations.

    The table schema matches the DataFrame columns exactly.
    First run creates the table; subsequent runs append by default.

    Args:
        df:        Output of transformer.build_long_dataframe().
        overwrite: If True, TRUNCATE the table before loading.
                   Use this when re-running a full historical backfill.
    """
    try:
        import duckdb
    except ImportError:
        raise ImportError("duckdb is not installed.\nRun: pip install duckdb")

    log.info(
        f"Connecting to MotherDuck: "
        f"{MOTHERDUCK_DATABASE}.{MOTHERDUCK_SCHEMA}.{MOTHERDUCK_TABLE}"
    )
    con = _connect()

    try:
        _validate_identifier(MOTHERDUCK_TABLE)

        con.execute(f"CREATE SCHEMA IF NOT EXISTS {MOTHERDUCK_SCHEMA}")

        # Create table if it doesn't exist (idempotent)
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {MOTHERDUCK_SCHEMA}.{MOTHERDUCK_TABLE} (
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
                INGESTED_AT     TIMESTAMPTZ
            )
        """)

        if overwrite:
            con.execute(f"TRUNCATE TABLE {MOTHERDUCK_SCHEMA}.{MOTHERDUCK_TABLE}")
            log.info(f"Table {MOTHERDUCK_TABLE} truncated (overwrite=True)")

        # Match the MotherDuck table's UPPERCASE column names
        df_upload = df.copy()
        df_upload.columns = df_upload.columns.str.upper()

        con.execute(f"""
            INSERT INTO {MOTHERDUCK_SCHEMA}.{MOTHERDUCK_TABLE}
            SELECT
                PROJECT_ID, TITLE, STATUS, LEAD_FUNDER, GRANT_CATEGORY,
                START_DATE::DATE, END_DATE::DATE, CATEGORY, KEYWORD, FOUND_IN,
                GTR_URL, INGESTED_AT::TIMESTAMPTZ
            FROM df_upload
        """)

        log.info(f"MotherDuck load complete — {len(df_upload):,} rows")

    finally:
        con.close()


def load_all_projects_to_motherduck(df: pd.DataFrame, overwrite: bool = False) -> None:
    """
    Load the wide-format all-funders DataFrame into UKRI_ALL_PROJECTS (RAW layer).
    Schema: one row per project — no keyword expansion.
    """
    try:
        import duckdb
    except ImportError:
        raise ImportError("duckdb is not installed.\nRun: pip install duckdb")

    _validate_identifier(RPI_MOTHERDUCK_TABLE)
    log.info(
        f"Connecting to MotherDuck: "
        f"{MOTHERDUCK_DATABASE}.{MOTHERDUCK_SCHEMA}.{RPI_MOTHERDUCK_TABLE}"
    )
    con = _connect()
    try:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {MOTHERDUCK_SCHEMA}")
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {MOTHERDUCK_SCHEMA}.{RPI_MOTHERDUCK_TABLE} (
                PROJECT_ID      VARCHAR,
                TITLE           VARCHAR,
                STATUS          VARCHAR,
                LEAD_FUNDER     VARCHAR,
                GRANT_CATEGORY  VARCHAR,
                DEPARTMENT      VARCHAR,
                START_DATE      DATE,
                END_DATE        DATE,
                GTR_URL         VARCHAR,
                INGESTED_AT     TIMESTAMPTZ
            )
        """)
        if overwrite:
            con.execute(f"TRUNCATE TABLE {MOTHERDUCK_SCHEMA}.{RPI_MOTHERDUCK_TABLE}")
            log.info(f"Table {RPI_MOTHERDUCK_TABLE} truncated (overwrite=True)")

        df_upload = df.copy()
        df_upload.columns = df_upload.columns.str.upper()
        con.execute(f"""
            INSERT INTO {MOTHERDUCK_SCHEMA}.{RPI_MOTHERDUCK_TABLE}
            SELECT
                PROJECT_ID, TITLE, STATUS, LEAD_FUNDER, GRANT_CATEGORY, DEPARTMENT,
                START_DATE::DATE, END_DATE::DATE, GTR_URL, INGESTED_AT::TIMESTAMPTZ
            FROM df_upload
        """)
        log.info(f"RPI MotherDuck load complete — {len(df_upload):,} rows")
    finally:
        con.close()
