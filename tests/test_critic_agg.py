"""Tests for CriticProvider."""

import pytest

from movie_pipeline.config import MODEL_COLUMNS
from movie_pipeline.providers.critic import CriticProvider


class TestCriticAggProvider:

    def test_returns_correct_row_count(self, provider1_csv):
        df = CriticProvider(provider1_csv).extract_transform()
        # 4 rows in fixture, 1 malformed (no title) → 3 valid rows
        assert df.count() == 3

    def test_skips_rows_with_missing_title(self, provider1_csv):
        df = CriticProvider(provider1_csv).extract_transform()
        titles = [r["title"] for r in df.select("title").collect()]
        assert None not in titles

    def test_parses_title_correctly(self, provider1_csv):
        df = CriticProvider(provider1_csv).extract_transform()
        titles = {r["title"] for r in df.select("title").collect()}
        assert "Inception" in titles

    def test_parses_year_as_integer(self, provider1_csv):
        df = CriticProvider(provider1_csv).extract_transform()
        row = df.filter(df.title == "Inception").first()
        assert row["year"] == 2010

    def test_parses_critic_score_pct(self, provider1_csv):
        df = CriticProvider(provider1_csv).extract_transform()
        row = df.filter(df.title == "Inception").first()
        assert row["critic_score_pct"] == pytest.approx(87.0)

    def test_parses_total_critic_reviews(self, provider1_csv):
        df = CriticProvider(provider1_csv).extract_transform()
        row = df.filter(df.title == "Inception").first()
        assert row["total_critic_reviews"] == 450

    def test_output_has_all_canonical_columns(self, provider1_csv):
        df = CriticProvider(provider1_csv).extract_transform()
        assert df.columns == MODEL_COLUMNS

    def test_non_critic_columns_are_null(self, provider1_csv):
        df = CriticProvider(provider1_csv).extract_transform()
        row = df.filter(df.title == "Inception").first()
        assert row["audience_avg_score"] is None
        assert row["domestic_gross_usd"] is None
