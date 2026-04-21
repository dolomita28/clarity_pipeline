"""Tests for transformation helper functions."""

import pytest
from pyspark.sql import functions as F
from pyspark.sql import types as T
from movie_pipeline.config import MODEL_COLUMNS
from movie_pipeline.transformations.helpers import enforce_schema, normalise_title


class TestNormaliseTitle:

    def test_lowercases(self, spark):
        result = spark.createDataFrame([("The Dark Knight",)], ["t"]) \
                      .select(normalise_title("t").alias("n")).first()["n"]
        assert result == "the dark knight"

    def test_trims_leading_and_trailing_whitespace(self, spark):
        result = spark.createDataFrame([("  Inception  ",)], ["t"]) \
                      .select(normalise_title("t").alias("n")).first()["n"]
        assert result == "inception"

    def test_collapses_internal_whitespace(self, spark):
        result = spark.createDataFrame([("The  Dark   Knight",)], ["t"]) \
                      .select(normalise_title("t").alias("n")).first()["n"]
        assert result == "the dark knight"

    def test_handles_already_clean_title(self, spark):
        result = spark.createDataFrame([("parasite",)], ["t"]) \
                      .select(normalise_title("t").alias("n")).first()["n"]
        assert result == "parasite"


class TestEnforceSchema:

    def test_adds_missing_canonical_columns(self, spark):
        df = spark.createDataFrame([("Inception", 2010)], ["title", "year"])
        out = enforce_schema(df)
        assert set(out.columns) == set(MODEL_COLUMNS)

    def test_drops_non_canonical_columns(self, spark):
        df = spark.createDataFrame([("Inception", 2010, "extra")], ["title", "year", "extra_col"])
        out = enforce_schema(df)
        assert "extra_col" not in out.columns

    def test_output_column_order_matches_canonical(self, spark):
        df = spark.createDataFrame([("Inception", 2010)], ["title", "year"])
        out = enforce_schema(df)
        assert out.columns == MODEL_COLUMNS

    def test_casts_year_to_integer(self, spark):
        df = spark.createDataFrame([("Inception", "2010")], ["title", "year"])
        out = enforce_schema(df)        
        assert isinstance(out.schema["year"].dataType, T.IntegerType)

    def test_casts_domestic_gross_to_long(self, spark):
        df = spark.createDataFrame(
            [("Inception", 2010, "292576195")],
            ["title", "year", "domestic_gross_usd"],
        )
        out = enforce_schema(df)        
        assert isinstance(out.schema["domestic_gross_usd"].dataType, T.LongType)

    def test_invalid_cast_becomes_null(self, spark):
        df = spark.createDataFrame([("Inception", "not_a_year")], ["title", "year"])
        out = enforce_schema(df)
        assert out.first()["year"] is None
