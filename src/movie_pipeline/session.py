"""
SparkSession factory.

Centralising session creation means:
- Tests can call get_spark() with a test-specific app name without
  touching any provider or pipeline code.
- Swapping local[*] for a cluster URL only requires changing config.py
  (or the SPARK_MASTER env var) — nothing else.
"""

from pyspark.sql import SparkSession

from movie_pipeline.config import SPARK_APP_NAME, SPARK_MASTER, SPARK_SHUFFLE_PARTITIONS


def get_spark(app_name: str = SPARK_APP_NAME, master: str = SPARK_MASTER) -> SparkSession:
    """
    Return an existing SparkSession or create a new one.

    PySpark's builder pattern is idempotent — calling this multiple times
    returns the same session as long as the JVM is alive.
    """
    return (
        SparkSession.builder
        .master(master)
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", str(SPARK_SHUFFLE_PARTITIONS))
        .config("spark.ui.showConsoleProgress", "false")
        # Disable ANSI mode so invalid casts produce NULL instead of raising.
        # Matches pandas' errors='coerce' behaviour.
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )
