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

    href: Optional[str] = None
    created: Optional[int] = None

    # Fund dates live in links.link[rel=FUND] as Unix ms timestamps, not top-level fields.
    # Populated by _extract_fund_dates after validation.
    start: Optional[int] = Field(default=None, exclude=True)
    end: Optional[int] = Field(default=None, exclude=True)
    links: Optional[Any] = Field(default=None, exclude=True)

    @field_validator("title", "abstractText", "techAbstractText", "potentialImpact", mode="before")
    @classmethod
    def decode_html_entities(cls, v):
        return unescape(v) if isinstance(v, str) else v

    @model_validator(mode="after")
    def _extract_fund_dates(self) -> "UKRIProjectRecord":
        """Pull start/end timestamps from the FUND link if not already set."""
        if self.start is not None and self.end is not None:
            return self
        links_data = self.links
        if not links_data:
            return self
        for link in links_data.get("link", []):
            if link.get("rel") == "FUND":
                if self.start is None:
                    self.start = link.get("start")
                if self.end is None:
                    self.end = link.get("end")
                break
        return self

    # ── Computed properties ────────────────────────────────────────────────────

    @property
    def combined_text(self) -> str:
        parts = [
            self.abstractText or "",
            self.techAbstractText or "",
            self.potentialImpact or "",
        ]
        return " ".join(p for p in parts if p).lower()

    @property
    def start_date(self) -> Optional[date]:
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
        return f"https://gtr.ukri.org/project/{self.id}"


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
