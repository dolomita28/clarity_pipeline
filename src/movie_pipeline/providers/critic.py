from pyspark.sql import DataFrame, functions as F

from movie_pipeline.providers.base import BaseProvider
from movie_pipeline.schema import CRITIC_SCHEMA
from movie_pipeline.session import get_spark
from movie_pipeline.transformations.helpers import enforce_schema


class CriticProvider(BaseProvider):
    """
    Reads critic score data from a single CSV file.    
    """

    def __init__(self, filepath: str) -> None:
        self.filepath = filepath

    def extract_transform(self) -> DataFrame:
        spark = get_spark()
        # read from file
        raw = spark.read.option("header", True).schema(CRITIC_SCHEMA).csv(self.filepath)

        # filter at least having title and year, then rename columns
        df = (
            raw
            .filter(F.col("movie_title").isNotNull() & F.col("release_year").isNotNull())
            .withColumnRenamed("movie_title",                  "title")
            .withColumnRenamed("release_year",                 "year")
            .withColumnRenamed("critic_score_percentage",       "critic_score_pct")
            .withColumnRenamed("total_critic_reviews_counted", "total_critic_reviews")
        )

        return enforce_schema(df)
