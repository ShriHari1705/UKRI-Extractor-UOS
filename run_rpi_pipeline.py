#!/usr/bin/env python3
"""
run_rpi_pipeline.py — UKRI RPI Growth Manager Data Pipeline
============================================================
Fetches ALL UKRI projects (all funders — EPSRC, MRC, BBSRC, AHRC, ESRC,
NERC, STFC, Innovate UK, etc.) and loads a wide-format project metadata
table into MotherDuck for the RPI Growth Manager dashboard tab.

No keyword tagging. One row per project.

Usage
-----
    python run_rpi_pipeline.py                          # CSV output only
    python run_rpi_pipeline.py --motherduck             # load to MotherDuck
    python run_rpi_pipeline.py --motherduck --overwrite # truncate first (full reload)
    python run_rpi_pipeline.py --pages 20                # dev mode

Output
------
    outputs/ukri_all_projects_YYYYMMDD_HHMM.csv
    MotherDuck: UKRI_EWS.RAW.UKRI_ALL_PROJECTS

Note: Reuses the local page cache from run_pipeline.py — no re-fetching needed
if the main pipeline has already run.
"""
import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

from config import USE_S3, OUTPUT_DIR
from pipeline.fetcher import fetch_all_pages
from pipeline.rpi_transformer import build_all_projects_dataframe

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(stream=open(sys.stdout.fileno(), mode="w", encoding="utf-8", closefd=False)),
        logging.FileHandler("rpi_pipeline.log", mode="a", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)


def print_summary(df) -> None:
    if df.empty:
        print("\nNo data produced.\n")
        return

    w = 60
    print("\n" + "═" * w)
    print("  UKRI RPI PIPELINE — RUN SUMMARY")
    print("═" * w)
    print(f"  Total projects loaded      : {len(df):>6,}")
    print(f"  Unique funders             : {df['lead_funder'].nunique():>6,}")
    print("─" * w)
    top = df.groupby("lead_funder").size().sort_values(ascending=False).head(10)
    for funder, count in top.items():
        print(f"  {str(funder):<40}: {count:>5,}")
    print("═" * w + "\n")


def main():
    parser = argparse.ArgumentParser(
        description="UKRI RPI Pipeline — load all-funder project metadata"
    )
    parser.add_argument("--pages", type=int, default=None,
                        help="Limit to first N pages (dev mode)")
    parser.add_argument("--motherduck", action="store_true",
                        help="Load to MotherDuck (UKRI_ALL_PROJECTS)")
    parser.add_argument("--overwrite", action="store_true",
                        help="Truncate table before loading")
    args = parser.parse_args()

    log.info("═══ UKRI RPI Pipeline — Starting ═══")

    s3_client = None
    if USE_S3:
        import boto3, os
        s3_client = boto3.client("s3", region_name=os.getenv("AWS_DEFAULT_REGION", "eu-west-2"))

    pages = fetch_all_pages(max_pages=args.pages, s3_client=s3_client)
    df = build_all_projects_dataframe(pages, motherduck=args.motherduck, overwrite=args.overwrite)

    if not df.empty:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        ts   = datetime.now().strftime("%Y%m%d_%H%M")
        path = OUTPUT_DIR / f"ukri_all_projects_{ts}.csv"
        df.to_csv(path, index=False)
        log.info(f"CSV saved → {path}")
        print_summary(df)

    log.info("═══ RPI Pipeline complete ═══")


if __name__ == "__main__":
    main()
