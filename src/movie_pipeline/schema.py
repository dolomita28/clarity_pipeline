"""
Spark schemas for the pipeline.

Keeping schemas here (rather than inline in providers) means:
- A single place to update when a provider changes their column names or types.
- Providers stay thin — they only handle renaming and filtering.
- Tests can import schemas directly to assert on structure.
"""

from pyspark.sql import types as T

# ── Canonical output schema ───────────────────────────────────────────────────
# Every provider must produce a DataFrame that conforms to this schema after
# transformation. Missing columns are added as NULL by enforce_schema().

MODEL_SCHEMA = T.StructType([
    T.StructField("title",                   T.StringType(),  nullable=True),
    T.StructField("year",                    T.IntegerType(), nullable=True),
    T.StructField("critic_score_pct",        T.DoubleType(),  nullable=True),
    T.StructField("top_critic_score",        T.DoubleType(),  nullable=True),
    T.StructField("total_critic_reviews",    T.LongType(),    nullable=True),
    T.StructField("audience_avg_score",      T.DoubleType(),  nullable=True),
    T.StructField("total_audience_ratings",  T.LongType(),    nullable=True),
    T.StructField("domestic_gross_usd",      T.LongType(),    nullable=True),
    T.StructField("international_gross_usd", T.LongType(),    nullable=True),
    T.StructField("production_budget_usd",   T.LongType(),    nullable=True),
    T.StructField("marketing_spend_usd",     T.LongType(),    nullable=True),
])

# ── Provider 1: CriticAgg ─────────────────────────────────────────────────────

CRITIC_SCHEMA = T.StructType([
    T.StructField("movie_title",                     T.StringType(),  True),
    T.StructField("release_year",                    T.IntegerType(), True),
    T.StructField("critic_score_percentage",          T.DoubleType(),  True),
    T.StructField("top_critic_score",                T.DoubleType(),  True),
    T.StructField("total_critic_reviews_counted",    T.LongType(),    True),
])

# ── Provider 2: AudiencePulse ─────────────────────────────────────────────────

AUDIENCE_SCHEMA = T.StructType([
    T.StructField("title",                      T.StringType(), True),
    T.StructField("year",                       T.StringType(), True),  # arrives as string
    T.StructField("audience_average_score",     T.DoubleType(), True),
    T.StructField("total_audience_ratings",     T.LongType(),   True),
    T.StructField("domestic_box_office_gross",  T.LongType(),   True),
])

# ── Provider 3: BoxOfficeMetrics ──────────────────────────────────────────────

BOX_OFFICE_SCHEMA = T.StructType([
    T.StructField("film_name",            T.StringType(),  True),
    T.StructField("year_of_release",      T.IntegerType(), True),
    T.StructField("box_office_gross_usd", T.LongType(),    True),
])

FINANCIALS_SCHEMA = T.StructType([
    T.StructField("film_name",             T.StringType(),  True),
    T.StructField("year_of_release",       T.IntegerType(), True),
    T.StructField("production_budget_usd", T.LongType(),    True),
    T.StructField("marketing_spend_usd",   T.LongType(),    True),
])
