"""
transformer.py — Filters UKRI projects by funder and tags abstracts with keywords.

Output data model: LONG FORMAT — one row per (project × keyword) pair.

Why long format (from notebook exploration):
  Wide format (boolean flag per keyword) makes questions like
  "how many projects mention GPU?" require messy string parsing.
  Long format lets you answer that with a simple GROUP BY.

  Wide:  project_id | tag_ML_AI | tag_HPC | ...       ← hard to query
  Long:  project_id | category  | keyword | found_in  ← queryable in SQL/dbt
"""
import logging
from datetime import datetime, timezone
from typing import Generator, List

import pandas as pd
from pydantic import ValidationError

from config import TARGET_FUNDER, KEYWORD_TAXONOMY
from pipeline.models import UKRIPageResponse, UKRIProjectRecord

log = logging.getLogger(__name__)


# ── Parse ──────────────────────────────────────────────────────────────────────

def parse_projects(page: UKRIPageResponse) -> List[UKRIProjectRecord]:
    """
    Validate raw project dicts from a page response into UKRIProjectRecord objects.
    Bad records are skipped individually — one malformed record won't drop the page.
    """
    valid, skipped = [], 0
    for raw in page.project:
        try:
            valid.append(UKRIProjectRecord(**raw))
        except (ValidationError, Exception) as exc:
            skipped += 1
            log.warning(f"Skipping invalid project record: {exc}")
    if skipped:
        log.warning(f"Page {page.page}: skipped {skipped} invalid records")
    return valid


# ── Filter ─────────────────────────────────────────────────────────────────────

def filter_by_funder(
    projects: List[UKRIProjectRecord],
    funder: str = TARGET_FUNDER,
) -> List[UKRIProjectRecord]:
    """
    Keep only projects where leadFunder exactly matches the target funder.

    Why client-side filter: UKRI API v7 has no server-side funder filter endpoint.
    We must page through all projects and filter here.
    """
    return [p for p in projects if p.leadFunder == funder]


# ── Tag ────────────────────────────────────────────────────────────────────────

def tag_project(project: UKRIProjectRecord) -> List[dict]:
    """
    Match a project's combined text blob against KEYWORD_TAXONOMY.
    Returns one dict per keyword match (long format).

    Each row records:
        - which category the keyword belongs to
        - which specific keyword matched
        - which field(s) the keyword appeared in (abstractText / techAbstractText / potentialImpact)
          so we can later analyse whether tech_abstract has higher signal quality than general abstract

    Projects with NO keyword matches return an empty list —
    they are deliberately excluded from the output (no signal = not relevant to IT Services).
    """
    combined = project.combined_text  # already lowercased
    rows = []

    for category, keywords in KEYWORD_TAXONOMY.items():
        for kw in keywords:
            if kw not in combined:
                continue

            # Track per-field presence for signal quality analysis
            found_in = []
            if project.abstractText and kw in (project.abstractText or "").lower():
                found_in.append("abstractText")
            if project.techAbstractText and kw in (project.techAbstractText or "").lower():
                found_in.append("techAbstractText")
            if project.potentialImpact and kw in (project.potentialImpact or "").lower():
                found_in.append("potentialImpact")

            rows.append({
                "project_id":     project.id,
                "title":          project.title,
                "status":         project.status,
                "lead_funder":    project.leadFunder,
                "grant_category": project.grantCategory,
                "start_date":     project.start_date,   # Python date or None
                "end_date":       project.end_date,
                "category":       category,
                "keyword":        kw,
                "found_in":       ", ".join(found_in),
                "gtr_url":        project.gtr_url,
                "ingested_at":    datetime.now(tz=timezone.utc).isoformat(),
            })

    return rows


# ── Orchestrate ────────────────────────────────────────────────────────────────

def build_long_dataframe(pages: Generator) -> pd.DataFrame:
    """
    Consume all pages from the fetcher, filter by funder, tag, and return
    a long-format DataFrame ready for CSV or Snowflake load.

    Args:
        pages: Generator of UKRIPageResponse (from fetcher.fetch_all_pages).

    Returns:
        pd.DataFrame — one row per (project × keyword) pair.
        Empty DataFrame if no matches found (check TARGET_FUNDER in config.py).
    """
    all_rows: list[dict] = []
    total_seen = 0
    total_matched = 0

    for page in pages:
        projects = parse_projects(page)
        total_seen += len(projects)

        matched = filter_by_funder(projects)
        total_matched += len(matched)

        for proj in matched:
            all_rows.extend(tag_project(proj))

        log.info(
            f"Page {page.page:>4}/{page.totalPages}  |  "
            f"seen: {total_seen:>6,}  |  "
            f"funder matches: {total_matched:>4}  |  "
            f"tag rows so far: {len(all_rows):>6,}"
        )

    if not all_rows:
        log.warning(
            f"No rows produced. "
            f"Check TARGET_FUNDER='{TARGET_FUNDER}' matches the API's leadFunder field exactly."
        )
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)
    df["start_date"] = pd.to_datetime(df["start_date"], errors="coerce")
    df["end_date"]   = pd.to_datetime(df["end_date"],   errors="coerce")
    df = df.sort_values(
        ["start_date", "project_id", "category", "keyword"],
        ascending=[False, True, True, True],
    ).reset_index(drop=True)

    log.info(
        f"Transform complete — "
        f"{total_matched} {TARGET_FUNDER} projects, "
        f"{df['project_id'].nunique()} with keyword matches, "
        f"{len(df):,} total tag rows"
    )
    return df
