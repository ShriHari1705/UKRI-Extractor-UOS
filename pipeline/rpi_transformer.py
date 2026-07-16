"""
rpi_transformer.py — Extracts project metadata for ALL UKRI funders.

No funder filter. No keyword tagging. One row per project.
Output feeds UKRI_ALL_PROJECTS → mart_rpi_funding_landscape → RPI dashboard tab.

Why separate from transformer.py:
  The IT Services pipeline needs long-format (project × keyword) rows.
  The RPI pipeline needs wide-format (one row per project) with funder metadata.
  Mixing the two would make both harder to reason about.
"""
import logging
from datetime import datetime, timezone
from typing import Generator

import pandas as pd
from pydantic import ValidationError

from pipeline.fetcher import fetch_all_pages
from pipeline.models import UKRIPageResponse, UKRIProjectRecord

log = logging.getLogger(__name__)

FLUSH_EVERY = 500  # pages


def _safe_date(project, attr: str):
    try:
        val = getattr(project, attr)
        return str(val) if val else None
    except (OSError, OverflowError, ValueError):
        return None


def _extract_row(project: UKRIProjectRecord) -> dict:
    return {
        "project_id":     project.id,
        "title":          project.title,
        "status":         project.status,
        "lead_funder":    project.leadFunder,
        "grant_category": project.grantCategory,
        "department":     project.leadOrganisationDepartment,
        "start_date":     _safe_date(project, "start_date"),
        "end_date":       _safe_date(project, "end_date"),
        "gtr_url":        project.gtr_url,
        "ingested_at":    datetime.now(tz=timezone.utc).isoformat(),
    }


def build_all_projects_dataframe(
    pages: Generator,
    motherduck: bool = False,
    overwrite: bool = False,
) -> pd.DataFrame:
    """
    Consume all pages, parse every project (all funders), return a wide-format
    DataFrame — one row per project.

    Args:
        pages:      Generator of UKRIPageResponse from fetcher.
        motherduck: If True, flush to MotherDuck incrementally.
        overwrite:  Truncate UKRI_ALL_PROJECTS before first flush.
    """
    if motherduck:
        from pipeline.loader import load_all_projects_to_motherduck

    all_rows: list[dict] = []
    total_seen = 0
    first_flush = True

    for page in pages:
        valid, skipped = [], 0
        for raw in page.project:
            try:
                valid.append(UKRIProjectRecord(**raw))
            except (ValidationError, KeyError, TypeError, ValueError) as exc:
                skipped += 1
                log.debug(f"Skipping invalid record: {exc}")

        total_seen += len(valid)
        for proj in valid:
            all_rows.append(_extract_row(proj))

        if skipped:
            log.warning(f"Page {page.page}: skipped {skipped} invalid records")

        log.info(
            f"Page {page.page:>4}/{page.totalPages}  |  "
            f"total parsed: {total_seen:>6,}  |  "
            f"rows buffered: {len(all_rows):>6,}"
        )

        if motherduck and page.page % FLUSH_EVERY == 0 and all_rows:
            flush_df = pd.DataFrame(all_rows)
            load_all_projects_to_motherduck(flush_df, overwrite=(overwrite and first_flush))
            log.info(f"Incremental flush — {len(flush_df):,} rows at page {page.page}")
            all_rows = []
            first_flush = False

    if motherduck and all_rows:
        flush_df = pd.DataFrame(all_rows)
        load_all_projects_to_motherduck(flush_df, overwrite=(overwrite and first_flush))
        log.info(f"Final flush — {len(flush_df):,} rows")
        all_rows = []

    if not all_rows:
        log.info(f"RPI transform complete — {total_seen:,} projects written to MotherDuck")
        return pd.DataFrame(columns=[
            "project_id", "title", "status", "lead_funder", "grant_category",
            "department", "start_date", "end_date", "gtr_url", "ingested_at",
        ])

    df = pd.DataFrame(all_rows)
    log.info(f"RPI transform complete — {total_seen:,} projects, {df['lead_funder'].nunique()} funders")
    return df
