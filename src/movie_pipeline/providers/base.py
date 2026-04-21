"""
Abstract base class for all data providers.

Every provider must implement a single method: extract_transform().

To add a new provider:
    1. Create a new file in this folder.
    2. Subclass BaseProvider.
    3. Implement extract_transform() — return a model DataFrame.
    4. Add it to the providers list in pipeline.py.
"""

import abc

from pyspark.sql import DataFrame


class BaseProvider(abc.ABC):
    """Contract every data provider must follow."""

    @abc.abstractmethod
    def extract_transform(self) -> DataFrame:
        """
        Read raw data from the source, rename/cast columns to the
        canonical schema, and return a clean DataFrame.

        Must always return a DataFrame — never None.
        Missing canonical columns are handled downstream by enforce_schema().
        """
