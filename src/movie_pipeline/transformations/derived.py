"""
Derived column computations applied after the merge step.

Keeping these separate from the merge logic means:
- Easy to add new derived metrics without touching the merge code.
- Each computation is independently testable.
"""

from pyspark.sql import DataFrame, functions as F


def add_worldwide_gross(df: DataFrame) -> DataFrame:
    """
    worldwide_gross_usd = domestic_gross_usd + international_gross_usd.
    """
    return df.withColumn(
        "worldwide_gross_usd",
        F.col("domestic_gross_usd") + F.col("international_gross_usd"),
    )


def add_roi(df: DataFrame) -> DataFrame:
    """
    ROI = (worldwide_gross - total_cost) / total_cost, rounded to 4 dp.
    NULL when worldwide_gross is unknown or total_cost is zero.

    total_cost = production_budget + marketing_spend
    (each component defaults to 0 if missing so a partial cost still works).
    """
    df = df.withColumn(
        "_total_cost",
        F.coalesce(F.col("production_budget_usd"), F.lit(0)) +
        F.coalesce(F.col("marketing_spend_usd"), F.lit(0)),
    )
    df = df.withColumn(
        "roi",
        F.when(
            F.col("_total_cost") > 0,
            F.round(
                (F.col("worldwide_gross_usd") - F.col("_total_cost")) /
                F.col("_total_cost"),
                4,
            ),
        ),
    )
    return df.drop("_total_cost")


def add_derived_columns(df: DataFrame) -> DataFrame:
    """Apply all derived column transformations in order."""
    df = add_worldwide_gross(df)
    df = add_roi(df)
    return df
