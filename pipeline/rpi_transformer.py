"""
rpi_transformer.py — Extracts project metadata for ALL UKRI funders.

No funder filter. One row per project (wide format) for the RPI dashboard,
plus one row per (project × keyword) match (long format, reusing
transformer.tag_project) for the RPI keyword-tag mart.

Output feeds:
  UKRI_ALL_PROJECTS      → mart_rpi_funding_landscape → RPI dashboard tab
  UKRI_ALL_PROJECTS_TAGS → mart_rpi_keyword_tags      → RPI category breakdowns

Why separate from transformer.py:
  The IT Services pipeline needs long-format (project × keyword) rows,
  restricted to Innovate UK. The RPI pipeline needs the same tagging logic
  but across all funders, plus the wide-format project metadata. Mixing the
  two would make both harder to reason about.
"""
import logging
from datetime import datetime, timezone
from typing import Generator

import pandas as pd
from pydantic import ValidationError

from pipeline.fetcher import fetch_all_pages
from pipeline.models import UKRIPageResponse, UKRIProjectRecord
from pipeline.transformer import tag_project

log = logging.getLogger(__name__)

FLUSH_EVERY = 500  # pages

PROJECTS_COLUMNS = [
    "project_id", "title", "status", "lead_funder", "grant_category",
    "department", "start_date", "end_date", "gtr_url", "ingested_at",
]
TAGS_COLUMNS = [
    "project_id", "title", "status", "lead_funder", "grant_category",
    "start_date", "end_date", "category", "keyword", "found_in",
    "gtr_url", "ingested_at",
]


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


def _stringify_tag_dates(df: pd.DataFrame) -> pd.DataFrame:
    df["start_date"] = df["start_date"].apply(lambda x: str(x) if x is not None else None)
    df["end_date"]   = df["end_date"].apply(lambda x: str(x) if x is not None else None)
    return df


def build_all_projects_dataframe(
    pages: Generator,
    motherduck: bool = False,
    overwrite: bool = False,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Consume all pages, parse every project (all funders), and return:
      - wide-format DataFrame — one row per project
      - long-format tag DataFrame — one row per (project × keyword) match,
        across all funders (via transformer.tag_project)

    Args:
        pages:      Generator of UKRIPageResponse from fetcher.
        motherduck: If True, flush to MotherDuck incrementally.
        overwrite:  Truncate UKRI_ALL_PROJECTS / UKRI_ALL_PROJECTS_TAGS before first flush.
    """
    if motherduck:
        from pipeline.loader import load_all_projects_to_motherduck, load_all_tags_to_motherduck

    all_rows: list[dict] = []
    all_tag_rows: list[dict] = []
    total_seen = 0
    first_flush = True

    def _flush(do_overwrite: bool) -> None:
        nonlocal all_rows, all_tag_rows
        if all_rows:
            flush_df = pd.DataFrame(all_rows)
            load_all_projects_to_motherduck(flush_df, overwrite=do_overwrite)
            log.info(f"Incremental flush — {len(flush_df):,} project rows")
            all_rows = []
        if all_tag_rows:
            flush_tags_df = _stringify_tag_dates(pd.DataFrame(all_tag_rows))
            load_all_tags_to_motherduck(flush_tags_df, overwrite=do_overwrite)
            log.info(f"Incremental flush — {len(flush_tags_df):,} tag rows")
            all_tag_rows = []

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
            all_tag_rows.extend(tag_project(proj))

        if skipped:
            log.warning(f"Page {page.page}: skipped {skipped} invalid records")

        log.info(
            f"Page {page.page:>4}/{page.totalPages}  |  "
            f"total parsed: {total_seen:>6,}  |  "
            f"rows buffered: {len(all_rows):>6,}  |  "
            f"tag rows buffered: {len(all_tag_rows):>6,}"
        )

        if motherduck and page.page % FLUSH_EVERY == 0 and (all_rows or all_tag_rows):
            _flush(do_overwrite=(overwrite and first_flush))
            log.info(f"Flushed at page {page.page}")
            first_flush = False

    if motherduck and (all_rows or all_tag_rows):
        _flush(do_overwrite=(overwrite and first_flush))
        log.info("Final flush complete")

    if not all_rows:
        log.info(f"RPI transform complete — {total_seen:,} projects written to MotherDuck")
        projects_df = pd.DataFrame(columns=PROJECTS_COLUMNS)
    else:
        projects_df = pd.DataFrame(all_rows)
        log.info(f"RPI transform complete — {total_seen:,} projects, {projects_df['lead_funder'].nunique()} funders")

    if not all_tag_rows:
        tags_df = pd.DataFrame(columns=TAGS_COLUMNS)
    else:
        tags_df = _stringify_tag_dates(pd.DataFrame(all_tag_rows))
        log.info(f"RPI tag transform complete — {len(tags_df):,} tag rows, {tags_df['project_id'].nunique()} tagged projects")

    return projects_df, tags_df
