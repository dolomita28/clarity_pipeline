"""
Pipeline orchestrator.

Runs all providers, merges their outputs into a single DataFrame,
computes derived columns, and returns the unified result.

Can also be run directly as a script:
    python -m movie_pipeline.pipeline
"""

from pyspark.sql import DataFrame, functions as F

from movie_pipeline.config import (
    MODEL_COLUMNS,
    PROVIDER1_PATH,
    PROVIDER2_PATH,
    PROVIDER3_DOMESTIC_PATH,
    PROVIDER3_FINANCIALS_PATH,
    PROVIDER3_INTERNATIONAL_PATH,
)
from movie_pipeline.providers.base import BaseProvider
from movie_pipeline.providers.audience import AudienceProvider
from movie_pipeline.providers.box_office import BoxOfficeMetricsProvider
from movie_pipeline.providers.critic import CriticProvider
from movie_pipeline.transformations.derived import add_derived_columns
from movie_pipeline.transformations.helpers import normalise_title
from movie_pipeline.session import get_spark


def merge_providers(providers: list[BaseProvider]) -> DataFrame:
    """
    Run all providers and merge their DataFrames into one unified dataset.

    Steps:
        1. Run each provider and tag its rows with a priority index
           (lower index = higher priority when resolving field conflicts).
        2. Stack all DataFrames with unionByName — missing columns become NULL.
        3. Build a normalised merge key: (lower_title, year).
        4. groupBy merge key + agg with first(ignorenulls=True) — picks the
           first non-null value per column, equivalent to SQL COALESCE across rows.
        5. Compute derived columns (worldwide_gross_usd, roi).
        6. Drop internal bookkeeping columns and sort by year.

    Conflict resolution:
        When two providers both supply a value for the same field of the same
        movie, the provider that appears earlier in the list wins.
    """
    # Step 1 — run providers and tag with priority order
    tagged = [
        provider.extract_transform().withColumn("_order", F.lit(i))
        for i, provider in enumerate(providers)
    ]

    # Step 2 — stack all DataFrames
    combined = tagged[0]
    for df in tagged[1:]:
        combined = combined.unionByName(df, allowMissingColumns=True)

    # Step 3 — build merge key
    combined = combined.withColumn(
        "merge_key",
        F.concat_ws("__", normalise_title("title"), F.col("year").cast("string")),
    )

    # Step 4 — coalesce across rows per group
    agg_exprs = [
        F.first(F.col(c), ignorenulls=True).alias(c)
        for c in MODEL_COLUMNS
    ]
    coalesced = (
        combined
        .orderBy("_order")
        .groupBy("merge_key")
        .agg(*agg_exprs)
    )

    # Step 5 — derived columns
    unified = add_derived_columns(coalesced)

    # Step 6 — clean up and sort
    return unified.drop("merge_key","_order").orderBy("year")


def build_providers() -> list[BaseProvider]:
    """Instantiate all providers using paths from config."""
    return [
        CriticProvider(PROVIDER1_PATH),
        AudienceProvider(PROVIDER2_PATH),
        BoxOfficeMetricsProvider(
            PROVIDER3_DOMESTIC_PATH,
            PROVIDER3_INTERNATIONAL_PATH,
            PROVIDER3_FINANCIALS_PATH,
        ),
    ]


# Pipeline starting point
if __name__ == "__main__":
    
    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")

    providers = build_providers()
    unified = merge_providers(providers)

    print(f"\nPipeline complete — {unified.count()} movies in unified dataset.\n")
    unified.show(truncate=False)
    unified.printSchema()

    # from this point we could consider saving the dataframe to a Delta Table
    # unified.write.saveAsTable("Movies")

    spark.stop()
