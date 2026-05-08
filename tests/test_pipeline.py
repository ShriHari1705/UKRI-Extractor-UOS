"""
Unit tests for pipeline core logic: parsing, filtering, tagging, and loader validation.
Run with: pytest tests/
"""
import pytest
from datetime import timezone, datetime

from pipeline.models import UKRIProjectRecord, UKRIPageResponse
from pipeline.transformer import parse_projects, filter_by_funder, tag_project
from pipeline.loader import _validate_identifier


# ── Fixtures ───────────────────────────────────────────────────────────────────

def make_project(**overrides) -> dict:
    base = {
        "id": "proj-001",
        "title": "Test Project",
        "status": "Active",
        "leadFunder": "Innovate UK",
        "grantCategory": "Research Grant",
        "abstractText": "This project uses machine learning and neural networks.",
        "techAbstractText": "PyTorch on GPU cluster with CUDA.",
        "potentialImpact": "Accelerates deep learning workflows.",
        "start": 1669852800000,  # 2022-12-01
        "end":   1701388800000,  # 2023-12-01
    }
    base.update(overrides)
    return base


# ── UKRIProjectRecord ──────────────────────────────────────────────────────────

class TestUKRIProjectRecord:
    def test_valid_record_parses(self):
        r = UKRIProjectRecord(**make_project())
        assert r.id == "proj-001"
        assert r.leadFunder == "Innovate UK"

    def test_combined_text_is_lowercased(self):
        r = UKRIProjectRecord(**make_project(abstractText="Machine Learning"))
        assert "machine learning" in r.combined_text

    def test_combined_text_merges_all_fields(self):
        r = UKRIProjectRecord(**make_project(
            abstractText="genomics",
            techAbstractText="kubernetes",
            potentialImpact="terraform",
        ))
        assert "genomics" in r.combined_text
        assert "kubernetes" in r.combined_text
        assert "terraform" in r.combined_text

    def test_start_date_converts_unix_ms(self):
        r = UKRIProjectRecord(**make_project(start=1669852800000))
        assert r.start_date is not None
        assert r.start_date.year == 2022

    def test_end_date_none_when_missing(self):
        r = UKRIProjectRecord(**make_project(end=None))
        assert r.end_date is None

    def test_start_date_none_when_missing(self):
        r = UKRIProjectRecord(**make_project(start=None))
        assert r.start_date is None

    def test_gtr_url_contains_id(self):
        r = UKRIProjectRecord(**make_project(id="abc-123"))
        assert "abc-123" in r.gtr_url

    def test_extra_fields_ignored(self):
        r = UKRIProjectRecord(**make_project(unknownField="ignored"))
        assert not hasattr(r, "unknownField")

    def test_missing_required_id_raises(self):
        data = make_project()
        del data["id"]
        with pytest.raises(Exception):
            UKRIProjectRecord(**data)


# ── parse_projects ─────────────────────────────────────────────────────────────

class TestParseProjects:
    def _make_page(self, projects: list) -> UKRIPageResponse:
        return UKRIPageResponse(page=1, size=len(projects), totalPages=1, totalSize=len(projects), project=projects)

    def test_valid_records_all_parsed(self):
        page = self._make_page([make_project(), make_project(id="proj-002")])
        result = parse_projects(page)
        assert len(result) == 2

    def test_malformed_record_skipped(self):
        bad = {"not_an_id": "missing required id field"}
        page = self._make_page([make_project(), bad])
        result = parse_projects(page)
        assert len(result) == 1

    def test_all_malformed_returns_empty(self):
        page = self._make_page([{"bad": "data"}, {"worse": "data"}])
        result = parse_projects(page)
        assert result == []

    def test_empty_page_returns_empty(self):
        page = self._make_page([])
        assert parse_projects(page) == []


# ── filter_by_funder ───────────────────────────────────────────────────────────

class TestFilterByFunder:
    def test_keeps_matching_funder(self):
        projects = [UKRIProjectRecord(**make_project(leadFunder="Innovate UK"))]
        result = filter_by_funder(projects, funder="Innovate UK")
        assert len(result) == 1

    def test_drops_non_matching_funder(self):
        projects = [UKRIProjectRecord(**make_project(leadFunder="EPSRC"))]
        result = filter_by_funder(projects, funder="Innovate UK")
        assert result == []

    def test_mixed_funders_filters_correctly(self):
        projects = [
            UKRIProjectRecord(**make_project(id="p1", leadFunder="Innovate UK")),
            UKRIProjectRecord(**make_project(id="p2", leadFunder="EPSRC")),
            UKRIProjectRecord(**make_project(id="p3", leadFunder="Innovate UK")),
        ]
        result = filter_by_funder(projects, funder="Innovate UK")
        assert len(result) == 2
        assert all(p.leadFunder == "Innovate UK" for p in result)

    def test_case_sensitive_match(self):
        projects = [UKRIProjectRecord(**make_project(leadFunder="innovate uk"))]
        result = filter_by_funder(projects, funder="Innovate UK")
        assert result == []

    def test_empty_list_returns_empty(self):
        assert filter_by_funder([], funder="Innovate UK") == []


# ── tag_project ────────────────────────────────────────────────────────────────

class TestTagProject:
    def test_keyword_match_returns_row(self):
        r = UKRIProjectRecord(**make_project(abstractText="machine learning model"))
        rows = tag_project(r)
        keywords = [row["keyword"] for row in rows]
        assert "machine learning" in keywords

    def test_no_match_returns_empty(self):
        r = UKRIProjectRecord(**make_project(
            abstractText="water treatment plant",
            techAbstractText="",
            potentialImpact="",
        ))
        assert tag_project(r) == []

    def test_row_contains_expected_fields(self):
        r = UKRIProjectRecord(**make_project(abstractText="deep learning"))
        rows = tag_project(r)
        assert rows, "Expected at least one match"
        row = rows[0]
        for field in ("project_id", "title", "category", "keyword", "found_in", "gtr_url", "ingested_at"):
            assert field in row, f"Missing field: {field}"

    def test_found_in_tracks_correct_field(self):
        r = UKRIProjectRecord(**make_project(
            abstractText="deep learning",
            techAbstractText="",
            potentialImpact="",
        ))
        rows = [row for row in tag_project(r) if row["keyword"] == "deep learning"]
        assert rows
        assert "abstractText" in rows[0]["found_in"]
        assert "techAbstractText" not in rows[0]["found_in"]

    def test_multiple_categories_tagged(self):
        r = UKRIProjectRecord(**make_project(
            abstractText="machine learning on kubernetes cluster"
        ))
        categories = {row["category"] for row in tag_project(r)}
        assert "ML_AI" in categories
        assert "Cloud_Infrastructure" in categories

    def test_keyword_in_tech_abstract_only(self):
        r = UKRIProjectRecord(**make_project(
            abstractText="water treatment",
            techAbstractText="cuda gpu computing",
            potentialImpact="",
        ))
        rows = [row for row in tag_project(r) if row["keyword"] == "cuda"]
        assert rows
        assert "techAbstractText" in rows[0]["found_in"]

    def test_category_assigned_correctly(self):
        r = UKRIProjectRecord(**make_project(abstractText="slurm hpc cluster"))
        rows = [row for row in tag_project(r) if row["keyword"] == "slurm"]
        assert rows
        assert rows[0]["category"] == "HPC_Simulation"


# ── _validate_identifier ───────────────────────────────────────────────────────

class TestValidateIdentifier:
    def test_valid_table_name_passes(self):
        _validate_identifier("UKRI_PROJECTS")

    def test_lowercase_passes(self):
        _validate_identifier("ukri_projects")

    def test_name_with_numbers_passes(self):
        _validate_identifier("table_v2")

    def test_sql_injection_raises(self):
        with pytest.raises(ValueError):
            _validate_identifier("projects; DROP TABLE users--")

    def test_spaces_raise(self):
        with pytest.raises(ValueError):
            _validate_identifier("UKRI PROJECTS")

    def test_leading_number_raises(self):
        with pytest.raises(ValueError):
            _validate_identifier("1table")

    def test_empty_string_raises(self):
        with pytest.raises(ValueError):
            _validate_identifier("")

    def test_dot_notation_raises(self):
        with pytest.raises(ValueError):
            _validate_identifier("schema.table")
