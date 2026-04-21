"""End-to-end tests for merge_providers()."""

import pytest

from movie_pipeline.pipeline import merge_providers
from movie_pipeline.providers.audience import AudienceProvider
from movie_pipeline.providers.box_office import BoxOfficeMetricsProvider
from movie_pipeline.providers.critic import CriticProvider
from movie_pipeline.transformations.helpers import normalise_title
from pyspark.sql import functions as F


@pytest.fixture(scope="module")
def unified(provider1_csv, provider2_json, provider3_csvs):
    """
    Run the full pipeline once per module and cache the result.
    All tests in this file share this single DataFrame.
    """
    providers = [
        CriticProvider(provider1_csv),
        AudienceProvider(provider2_json),
        BoxOfficeMetricsProvider(**provider3_csvs),
    ]
    df = merge_providers(providers).cache()
    yield df
    df.unpersist()


def _row(unified, title: str, year: int):
    """Helper: fetch a single row by title + year."""
    return (
        unified
        .filter(
            (normalise_title("title") == title.lower().strip()) &
            (F.col("year") == year)
        )
        .first()
    )


# ── Row count ─────────────────────────────────────────────────────────────────

class TestRowCount:

    def test_exactly_three_movies(self, unified):
        assert unified.count() == 3

    def test_no_duplicate_rows(self, unified):
        assert unified.count() == unified.distinct().count()


# ── All movies present ────────────────────────────────────────────────────────

class TestMoviesPresent:

    def test_inception_present(self, unified):
        assert _row(unified, "inception", 2010) is not None

    def test_dark_knight_present(self, unified):
        assert _row(unified, "the dark knight", 2008) is not None

    def test_parasite_present(self, unified):
        assert _row(unified, "parasite", 2019) is not None


# ── Field values ──────────────────────────────────────────────────────────────

class TestFieldValues:

    def test_inception_critic_score(self, unified):
        assert _row(unified, "inception", 2010)["critic_score_pct"] == pytest.approx(87.0)

    def test_inception_audience_score(self, unified):
        assert _row(unified, "inception", 2010)["audience_avg_score"] == pytest.approx(9.1)

    def test_inception_domestic_gross(self, unified):
        assert _row(unified, "inception", 2010)["domestic_gross_usd"] == 292_576_195

    def test_inception_international_gross(self, unified):
        assert _row(unified, "inception", 2010)["international_gross_usd"] == 535_700_000

    def test_inception_production_budget(self, unified):
        assert _row(unified, "inception", 2010)["production_budget_usd"] == 160_000_000


# ── Derived columns ───────────────────────────────────────────────────────────

class TestDerivedColumns:

    def test_worldwide_gross_is_sum_of_domestic_and_international(self, unified):
        row = _row(unified, "inception", 2010)
        assert row["worldwide_gross_usd"] == 292_576_195 + 535_700_000

    def test_roi_formula(self, unified):
        row = _row(unified, "inception", 2010)
        gross = 292_576_195 + 535_700_000
        cost  = 160_000_000 + 100_000_000
        assert row["roi"] == pytest.approx((gross - cost) / cost, rel=1e-3)

    def test_worldwide_gross_null_when_international_missing(self, unified):
        """Parasite has no Provider 3 data → international gross is NULL → worldwide is NULL."""
        row = _row(unified, "parasite", 2019)
        assert row["worldwide_gross_usd"] is None

    def test_roi_null_when_cost_unknown(self, unified):
        row = _row(unified, "parasite", 2019)
        assert row["roi"] is None


# ── Conflict resolution ───────────────────────────────────────────────────────

class TestConflictResolution:

    def test_first_provider_wins_on_domestic_gross(self, unified):
        """
        Both Provider 2 (Audience) and Provider 3 (BoxOfficeMetrics)
        supply domestic_gross_usd for Inception. Provider 2 appears first
        in the list, so its value should be used.
        Both happen to agree in the sample data, confirming no corruption.
        """
        row = _row(unified, "inception", 2010)
        assert row["domestic_gross_usd"] == 292_576_195


# ── Partial data ──────────────────────────────────────────────────────────────

class TestPartialData:

    def test_parasite_has_critic_and_audience_data(self, unified):
        row = _row(unified, "parasite", 2019)
        assert row["critic_score_pct"] == pytest.approx(99.0)
        assert row["audience_avg_score"] == pytest.approx(9.0)

    def test_parasite_missing_financial_data(self, unified):
        """Parasite is absent from Provider 3 — financial fields must be NULL."""
        row = _row(unified, "parasite", 2019)
        assert row["production_budget_usd"] is None
        assert row["marketing_spend_usd"] is None
        assert row["international_gross_usd"] is None


# ── Ordering ──────────────────────────────────────────────────────────────────

class TestOrdering:

    def test_output_sorted_ascending_by_year(self, unified):
        years = [r["year"] for r in unified.select("year").collect()]
        assert years == sorted(years)


# ── Spark SQL interface ───────────────────────────────────────────────────────

class TestSparkSQL:

    def test_temp_view_is_queryable(self, unified, spark):
        unified.createOrReplaceTempView("movies_test")
        count = spark.sql("SELECT COUNT(*) AS n FROM movies_test").first()["n"]
        assert count == 3

    def test_sql_top_critic_score(self, unified, spark):
        unified.createOrReplaceTempView("movies_test")
        top = spark.sql(
            "SELECT title FROM movies_test ORDER BY critic_score_pct DESC LIMIT 1"
        ).first()["title"]
        assert top == "Parasite"
