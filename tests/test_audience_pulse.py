"""Tests for AudienceProvider."""

import pytest

from movie_pipeline.config import MODEL_COLUMNS
from movie_pipeline.providers.audience import AudienceProvider


class TestAudiencePulseProvider:

    def test_returns_correct_row_count(self, provider2_json):
        df = AudienceProvider(provider2_json).extract_transform()
        assert df.count() == 3

    def test_parses_title(self, provider2_json):
        df = AudienceProvider(provider2_json).extract_transform()
        titles = {r["title"] for r in df.select("title").collect()}
        assert "Inception" in titles
        assert "Parasite" in titles

    def test_parses_year_from_string(self, provider2_json):
        """year arrives as a string in the JSON — must be cast to int."""
        df = AudienceProvider(provider2_json).extract_transform()
        row = df.filter(df.title == "Inception").first()
        assert row["year"] == 2010

    def test_parses_audience_avg_score(self, provider2_json):
        df = AudienceProvider(provider2_json).extract_transform()
        row = df.filter(df.title == "Inception").first()
        assert row["audience_avg_score"] == pytest.approx(9.1)

    def test_parses_domestic_gross(self, provider2_json):
        df = AudienceProvider(provider2_json).extract_transform()
        row = df.filter(df.title == "Inception").first()
        assert row["domestic_gross_usd"] == 292_576_195

    def test_output_has_all_canonical_columns(self, provider2_json):
        df = AudienceProvider(provider2_json).extract_transform()
        assert df.columns == MODEL_COLUMNS

    def test_non_audience_columns_are_null(self, provider2_json):
        df = AudienceProvider(provider2_json).extract_transform()
        row = df.filter(df.title == "Inception").first()
        assert row["critic_score_pct"] is None
        assert row["international_gross_usd"] is None
