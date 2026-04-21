"""
Shared transformation helpers used by every provider.
"""

from pyspark.sql import DataFrame, functions as F

from movie_pipeline.config import CANONICAL_COLUMNS
from movie_pipeline.schema import CANONICAL_SCHEMA


def normalise_title(col_name: str) -> F.Column:
    """
    Spark column expression: lowercase, trim, collapse internal whitespace.

    Used to build a stable merge key that survives casing and spacing
    differences between providers.

    Example:
        "  The Dark Knight  " → "the dark knight"
    """
    return F.regexp_replace(F.trim(F.lower(F.col(col_name))), r"\s+", " ")


def enforce_schema(df: DataFrame) -> DataFrame:
    """
    Ensure a provider DataFrame fully conforms to the canonical schema.

    - Columns present in the canonical schema but absent from df are added as NULL.
    - Columns present in df but absent from the canonical schema are dropped.
    - Every column is cast to the type declared in CANONICAL_SCHEMA.

    This makes providers resilient to upstream schema changes: adding a new
    column to a source file only requires updating schema.py and the relevant
    provider — enforce_schema handles the rest automatically.
    """
    existing = set(df.columns)

    for field in CANONICAL_SCHEMA.fields:
        if field.name not in existing:
            df = df.withColumn(field.name, F.lit(None).cast(field.dataType))
        else:
            df = df.withColumn(field.name, F.col(field.name).cast(field.dataType))

    return df.select(CANONICAL_COLUMNS)
