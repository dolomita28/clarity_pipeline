"""
Central configuration for the pipeline.

All file paths, Spark tuning knobs, and column lists live here so that
changing an environment (local dev → staging → production) only requires
editing this one file (or overriding via environment variables).
"""

import os
import pathlib

# ── Directories ──────────────────────────────────────────────────────────────

ROOT_DIR = pathlib.Path(__file__).parent.parent.parent  # project root
DATA_DIR = pathlib.Path(os.getenv("DATA_DIR", ROOT_DIR / "data"))
OUTPUT_DIR = pathlib.Path(os.getenv("OUTPUT_DIR", ROOT_DIR / "output"))

# ── Provider file paths ───────────────────────────────────────────────────────

PROVIDER1_PATH = str(DATA_DIR / "provider1.csv")
PROVIDER2_PATH = str(DATA_DIR / "provider2.json")
PROVIDER3_DOMESTIC_PATH = str(DATA_DIR / "provider3_domestic.csv")
PROVIDER3_INTERNATIONAL_PATH = str(DATA_DIR / "provider3_international.csv")
PROVIDER3_FINANCIALS_PATH = str(DATA_DIR / "provider3_financials.csv")

# ── Spark settings ────────────────────────────────────────────────────────────

SPARK_APP_NAME = "MovieScorePipeline"
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")

# Keep partition count low for local dev; tune up for cluster runs.
SPARK_SHUFFLE_PARTITIONS = int(os.getenv("SPARK_SHUFFLE_PARTITIONS", "4"))

# ── Column lists ──────────────────────────────────────────────────────────────

# Names of every column the schema defines.
# Imported by providers and the merge step — single source of truth.
MODEL_COLUMNS = [
    "title",
    "year",
    "critic_score_pct",
    "top_critic_score",
    "total_critic_reviews",
    "audience_avg_score",
    "total_audience_ratings",
    "domestic_gross_usd",
    "international_gross_usd",
    "production_budget_usd",
    "marketing_spend_usd",
]
