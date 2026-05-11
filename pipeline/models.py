"""
models.py — Pydantic schemas for UKRI Gateway to Research API v7 responses.

These validate raw JSON at ingest time.
If the UKRI API changes its schema, Pydantic will raise a clear error here
rather than silently producing wrong data downstream — exactly the failure
mode Joe warned about ("the API isn't magnificently engineered on their side").

Schema derived from live API exploration (see UKRI_exploration.ipynb).
"""
from __future__ import annotations
from datetime import date, datetime, timezone
from html import unescape
from typing import Any, List, Optional

from pydantic import BaseModel, Field, field_validator, model_validator


class UKRIProjectRecord(BaseModel):
    """
    A single project record as returned by GET /projects?p=N (API v7).

    Extra fields from the API are silently ignored ('extra = ignore') so that
    new API fields don't break the pipeline — but unknown removals will still
    raise errors if a field we depend on disappears.
    """
    model_config = {"extra": "ignore"}

    id: str
    title: str
    status: Optional[str] = None
    grantCategory: Optional[str] = None
    leadFunder: Optional[str] = None
    leadOrganisationDepartment: Optional[str] = None

    # Three text fields — all searched, per Joe's advice.
    # techAbstractText is often more precise; potentialImpact adds extra signal.
    abstractText: Optional[str] = None
    techAbstractText: Optional[str] = None
    potentialImpact: Optional[str] = None

    # Dates are Unix millisecond timestamps in the raw API (e.g. 1669852800000).
    # They live on the project record directly (not inside a nested 'fund' dict).
    start: Optional[int] = None
    end: Optional[int] = None

    href: Optional[str] = None
    created: Optional[int] = None

    @field_validator("title", "abstractText", "techAbstractText", "potentialImpact", mode="before")
    @classmethod
    def decode_html_entities(cls, v):
        return unescape(v) if isinstance(v, str) else v

    # ── Computed properties ────────────────────────────────────────────────────

    @property
    def combined_text(self) -> str:
        """
        Merge all three text fields into one searchable blob (lowercased).
        This is the string that keyword matching runs against.
        """
        parts = [
            self.abstractText or "",
            self.techAbstractText or "",
            self.potentialImpact or "",
        ]
        return " ".join(p for p in parts if p).lower()

    @property
    def start_date(self) -> Optional[date]:
        """Convert Unix ms timestamp → Python date. Returns None if missing."""
        if self.start is None:
            return None
        return datetime.fromtimestamp(self.start / 1000, tz=timezone.utc).date()

    @property
    def end_date(self) -> Optional[date]:
        if self.end is None:
            return None
        return datetime.fromtimestamp(self.end / 1000, tz=timezone.utc).date()

    @property
    def gtr_url(self) -> str:
        return f"https://gtr.ukri.org/projects?ref={self.id}"


class UKRIPageResponse(BaseModel):
    """
    A single paginated response from GET /projects?p=N.

    Key pagination fields observed in live API:
        page, size, totalPages, totalSize
    The 'project' list contains raw dicts — validated individually in transformer.py
    so one bad record doesn't abort the whole page.
    """
    model_config = {"extra": "ignore"}

    page: int
    size: int
    totalPages: int
    totalSize: int
    project: List[Any] = Field(default_factory=list)
