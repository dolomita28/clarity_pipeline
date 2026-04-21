"""
Shared pytest fixtures.

SparkSession is created once for the entire test run — Spark startup is
expensive (~3 s) so we avoid recreating it per test or per module.

All data fixtures use scope="module" with tmp_path_factory so they can
be shared by the module-scoped `unified` fixture in test_pipeline.py.
"""

import json
import pytest
from movie_pipeline.session import get_spark


# ── SparkSession ─────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def spark():
    """Single SparkSession shared across the whole test run."""
    session = get_spark(app_name="MoviePipelineTests", master="local[2]")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


# ── Temp data file helpers ────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def provider1_csv(tmp_path_factory) -> str:
    path = tmp_path_factory.mktemp("p1") / "provider1.csv"
    path.write_text(
        "movie_title,release_year,critic_score_percentage,"
        "top_critic_score,total_critic_reviews_counted\n"
        "Inception,2010,87,8.1,450\n"
        "The Dark Knight,2008,94,8.6,350\n"
        "Parasite,2019,99,9.5,475\n"
        ",2021,50,6.0,10\n"
    )
    return str(path)


@pytest.fixture(scope="module")
def provider2_json(tmp_path_factory) -> str:
    path = tmp_path_factory.mktemp("p2") / "provider2.json"
    path.write_text(json.dumps([
        {"title": "Inception",       "year": "2010", "audience_average_score": 9.1,
         "total_audience_ratings": 1_500_000, "domestic_box_office_gross": 292_576_195},
        {"title": "The Dark Knight", "year": "2008", "audience_average_score": 9.4,
         "total_audience_ratings": 2_200_000, "domestic_box_office_gross": 533_345_358},
        {"title": "Parasite",        "year": "2019", "audience_average_score": 9.0,
         "total_audience_ratings":   800_000, "domestic_box_office_gross":  53_369_749},
    ], indent=2))
    return str(path)


@pytest.fixture(scope="module")
def provider3_csvs(tmp_path_factory) -> dict:
    base = tmp_path_factory.mktemp("p3")
    domestic = base / "provider3_domestic.csv"
    domestic.write_text(
        "film_name,year_of_release,box_office_gross_usd\n"
        "Inception,2010,292576195\n"
        "The Dark Knight,2008,533345358\n"
    )
    international = base / "provider3_international.csv"
    international.write_text(
        "film_name,year_of_release,box_office_gross_usd\n"
        "Inception,2010,535700000\n"
        "The Dark Knight,2008,469700000\n"
    )
    financials = base / "provider3_financials.csv"
    financials.write_text(
        "film_name,year_of_release,production_budget_usd,marketing_spend_usd\n"
        "Inception,2010,160000000,100000000\n"
        "The Dark Knight,2008,185000000,150000000\n"
    )
    return {
        "domestic_path":      str(domestic),
        "international_path": str(international),
        "financials_path":    str(financials),
    }
