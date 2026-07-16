#!/usr/bin/env python3
"""
run_pipeline.py — UKRI Early Warning System — Entry Point
==========================================================
Fetches all UKRI projects from the Gateway to Research API,
filters for Innovate UK funded projects, tags abstracts by technology
category (ML/AI, HPC, Data-Intensive, Cloud), and saves a long-format
CSV (and optionally loads to MotherDuck).

Usage
-----
Local / HPC interactive:
    python run_pipeline.py                              # full run, CSV output
    python run_pipeline.py --pages 5                    # dev mode: first 5 pages only
    python run_pipeline.py --motherduck                 # also load to MotherDuck
    python run_pipeline.py --motherduck --overwrite     # truncate table first (backfill)
    python run_pipeline.py --recent --motherduck        # recent projects (2025+) sorted by start date
    python run_pipeline.py --recent --since 2024-01-01  # extend threshold back to 2024

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
from pipeline.fetcher import fetch_all_pages, fetch_recent_pages
from pipeline.transformer import build_long_dataframe
from pipeline.loader import save_csv, load_to_motherduck

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(stream=open(sys.stdout.fileno(), mode="w", encoding="utf-8", closefd=False)),
        logging.FileHandler("pipeline.log", mode="a", encoding="utf-8"),
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
        "--motherduck", action="store_true",
        help="Load output to MotherDuck after saving CSV",
    )
    parser.add_argument(
        "--overwrite", action="store_true",
        help="Truncate MotherDuck table before loading (use for full backfill)",
    )
    parser.add_argument(
        "--recent", action="store_true",
        help="Fetch projects sorted by start date desc, stopping at --since date",
    )
    parser.add_argument(
        "--since", type=str, default="2025-01-01",
        help="Stop recent fetch when project dates go before this date (YYYY-MM-DD). Default: 2025-01-01",
    )
    args = parser.parse_args()

    log.info("═══ UKRI Early Warning System Pipeline — Starting ═══")

    # S3 client (only needed if USE_S3=true in .env)
    s3_client = None
    if USE_S3:
        import boto3
        import os
        s3_client = boto3.client(
            "s3",
            region_name=os.getenv("AWS_DEFAULT_REGION", "eu-west-2"),
        )
        log.info("Storage mode: AWS S3")
    else:
        log.info("Storage mode: local disk cache")

    # ── Extract + Transform + incremental MotherDuck load ──────────────────────
    if args.recent:
        from datetime import datetime
        since_dt = datetime.strptime(args.since, "%Y-%m-%d")
        log.info(f"Recent mode: fetching projects with start date >= {args.since}")
        pages = fetch_recent_pages(since=since_dt, max_pages=args.pages, s3_client=s3_client)
    else:
        pages = fetch_all_pages(max_pages=args.pages, s3_client=s3_client)
    df = build_long_dataframe(
        pages,
        motherduck=args.motherduck,
        overwrite=args.overwrite,
    )

    if df.empty and not args.motherduck:
        log.error("Pipeline produced no output. Exiting.")
        sys.exit(1)

    # ── Final CSV save (skip if no local rows — all flushed incrementally) ────
    if not df.empty:
        save_csv(df)
        print_summary(df)
    else:
        log.info("All data written incrementally to MotherDuck — no local DataFrame to summarise")
    log.info("═══ Pipeline complete ═══")
    return df


if __name__ == "__main__":
    main()
