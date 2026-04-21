"""Provider 3: BoxOfficeMetrics — three monthly CSV files joined internally."""

from pyspark.sql import DataFrame, functions as F

from movie_pipeline.providers.base import BaseProvider
from movie_pipeline.schema import BOX_OFFICE_SCHEMA, FINANCIALS_SCHEMA
from movie_pipeline.session import get_spark
from movie_pipeline.transformations.helpers import enforce_schema


class BoxOfficeMetricsProvider(BaseProvider):
    """
    Joins three CSV files on (film_name, year_of_release):
      - domestic box office
      - international box office
      - production financials
    """

    def __init__(
        self,
        domestic_path: str,
        international_path: str,
        financials_path: str,
    ) -> None:
        self.domestic_path = domestic_path
        self.international_path = international_path
        self.financials_path = financials_path

    def _read_csv(self, path: str, schema) -> DataFrame:
        spark = get_spark()
        return spark.read.option("header", True).schema(schema).csv(path)

    def extract_transform(self) -> DataFrame:
        
        # consider three different files for this provider
        domestic = (
            self._read_csv(self.domestic_path, BOX_OFFICE_SCHEMA)
            .withColumnRenamed("box_office_gross_usd", "domestic_gross_usd")
        )
        international = (
            self._read_csv(self.international_path, BOX_OFFICE_SCHEMA)
            .withColumnRenamed("box_office_gross_usd", "international_gross_usd")
        )
        financials = self._read_csv(self.financials_path, FINANCIALS_SCHEMA)

        # key to perform joins
        key = ["film_name", "year_of_release"]

        # join three datasets, if a movie is missing from any file, it is included as null
        df = (
            domestic
            .join(international, on=key, how="outer")
            .join(financials, on=key, how="outer")
            .withColumnRenamed("film_name", "title")
            .withColumnRenamed("year_of_release", "year")
        )

        return enforce_schema(df)
