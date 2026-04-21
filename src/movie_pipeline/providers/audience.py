"""Provider 2: Audience — JSON array updated every 15 days."""

from pyspark.sql import DataFrame, functions as F

from movie_pipeline.providers.base import BaseProvider
from movie_pipeline.schema import AUDIENCE_SCHEMA
from movie_pipeline.session import get_spark
from movie_pipeline.transformations.helpers import enforce_schema


class AudienceProvider(BaseProvider):
    """
    Reads audience ratings and domestic box office data from a JSON file.

    The file is a JSON array , so multiLine=True is required.

    Source columns              → Model columns
    title                       → title
    year                        → year 
    audience_average_score      → audience_avg_score
    total_audience_ratings      → total_audience_ratings  (no rename needed)
    domestic_box_office_gross   → domestic_gross_usd
    """

    def __init__(self, filepath: str) -> None:
        self.filepath = filepath

    def extract_transform(self) -> DataFrame:
        spark = get_spark()

        # read data from json file
        raw = (
            spark.read
            .option("multiLine", True)  
            .schema(AUDIENCE_SCHEMA)
            .json(self.filepath)
        )
        # filter those rows where at least title and year are set, then rename columns
        df = (
            raw
            .filter(F.col("title").isNotNull() & F.col("year").isNotNull())
            .withColumnRenamed("audience_average_score",    "audience_avg_score")
            .withColumnRenamed("domestic_box_office_gross", "domestic_gross_usd")
        )
        # always enforce schema of the returned dataframe
        return enforce_schema(df)
