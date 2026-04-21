"""Tests for BoxOfficeMetricsProvider."""

import pytest

from movie_pipeline.config import MODEL_COLUMNS
from movie_pipeline.providers.box_office import BoxOfficeMetricsProvider


class TestBoxOfficeMetricsProvider:

    def test_returns_correct_row_count(self, provider3_csvs):
        df = BoxOfficeMetricsProvider(**provider3_csvs).extract_transform()
        assert df.count() == 2

    def test_parses_domestic_gross(self, provider3_csvs):
        df = BoxOfficeMetricsProvider(**provider3_csvs).extract_transform()
        row = df.filter(df.title == "Inception").first()
        assert row["domestic_gross_usd"] == 292_576_195

    def test_parses_international_gross(self, provider3_csvs):
        df = BoxOfficeMetricsProvider(**provider3_csvs).extract_transform()
        row = df.filter(df.title == "Inception").first()
        assert row["international_gross_usd"] == 535_700_000

    def test_parses_production_budget(self, provider3_csvs):
        df = BoxOfficeMetricsProvider(**provider3_csvs).extract_transform()
        row = df.filter(df.title == "Inception").first()
        assert row["production_budget_usd"] == 160_000_000

    def test_parses_marketing_spend(self, provider3_csvs):
        df = BoxOfficeMetricsProvider(**provider3_csvs).extract_transform()
        row = df.filter(df.title == "Inception").first()
        assert row["marketing_spend_usd"] == 100_000_000

    def test_outer_join_includes_all_films(self, provider3_csvs):
        """All films present in any of the three files must appear in output."""
        df = BoxOfficeMetricsProvider(**provider3_csvs).extract_transform()
        titles = {r["title"] for r in df.select("title").collect()}
        assert "Inception" in titles
        assert "The Dark Knight" in titles

    def test_output_has_all_canonical_columns(self, provider3_csvs):
        df = BoxOfficeMetricsProvider(**provider3_csvs).extract_transform()
        assert df.columns == MODEL_COLUMNS

    def test_non_box_office_columns_are_null(self, provider3_csvs):
        df = BoxOfficeMetricsProvider(**provider3_csvs).extract_transform()
        row = df.filter(df.title == "Inception").first()
        assert row["critic_score_pct"] is None
        assert row["audience_avg_score"] is None
