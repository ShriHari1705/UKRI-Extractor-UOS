#!/usr/bin/env python3
"""
run_pipeline.py — UKRI Early Warning System — Entry Point
==========================================================
Fetches all UKRI projects from the Gateway to Research API,
filters for Innovate UK funded projects, tags abstracts by technology
category (ML/AI, HPC, Data-Intensive, Cloud), and saves a long-format
CSV (and optionally loads to Snowflake).

Usage
-----
Local / HPC interactive:
    python run_pipeline.py                        # full run, CSV output
    python run_pipeline.py --pages 5              # dev mode: first 5 pages only
    python run_pipeline.py --snowflake            # also load to Snowflake
    python run_pipeline.py --snowflake --overwrite  # truncate table first (backfill)

HPC batch (SLURM):
    sbatch slurm_job.sh

Output
------
    outputs/ukri_innovate_uk_tagged_YYYYMMDD_HHMM.csv
    pipeline.log
"""
import argparse
import logging
import sys

from config import USE_S3
from pipeline.fetcher import fetch_all_pages
from pipeline.transformer import build_long_dataframe
from pipeline.loader import save_csv, load_to_snowflake

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("pipeline.log", mode="a"),
    ],
)
log = logging.getLogger(__name__)


def print_summary(df) -> None:
    if df.empty:
        print("\nNo data produced — check TARGET_FUNDER in config.py\n")
        return

    w = 60
    unique_projects = df["project_id"].nunique()
    print("\n" + "═" * w)
    print("  UKRI EARLY WARNING SYSTEM — RUN SUMMARY")
    print("═" * w)
    print(f"  Innovate UK projects with signals : {unique_projects:>6,}")
    print(f"  Total keyword-tag rows            : {len(df):>6,}")
    print("─" * w)
    for cat, grp in df.groupby("category"):
        n = grp["project_id"].nunique()
        print(f"  {cat:<35}: {n:>4} projects")
    print("═" * w + "\n")


def main():
    parser = argparse.ArgumentParser(
        description="UKRI Early Warning System — fetch, tag, and load Innovate UK projects"
    )
    parser.add_argument(
        "--pages", type=int, default=None,
        help="Limit fetch to first N pages (dev/test mode)",
    )
    parser.add_argument(
        "--snowflake", action="store_true",
        help="Load output to Snowflake after saving CSV",
    )
    parser.add_argument(
        "--overwrite", action="store_true",
        help="Truncate Snowflake table before loading (use for full backfill)",
    )
    args = parser.parse_args()

    log.info("═══ UKRI Early Warning System Pipeline — Starting ═══")

    # S3 client (only needed if USE_S3=true in .env)
    s3_client = None
    if USE_S3:
        import boto3
        s3_client = boto3.client("s3")
        log.info("Storage mode: AWS S3")
    else:
        log.info("Storage mode: local disk cache")

    # ── Extract ────────────────────────────────────────────────────────────────
    pages = fetch_all_pages(max_pages=args.pages, s3_client=s3_client)

    # ── Transform ──────────────────────────────────────────────────────────────
    df = build_long_dataframe(pages)

    if df.empty:
        log.error("Pipeline produced no output. Exiting.")
        sys.exit(1)

    # ── Load ───────────────────────────────────────────────────────────────────
    save_csv(df)

    if args.snowflake:
        load_to_snowflake(df, overwrite=args.overwrite)

    print_summary(df)
    log.info("═══ Pipeline complete ═══")
    return df


if __name__ == "__main__":
    main()
