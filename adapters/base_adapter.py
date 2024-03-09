# base_adapter.py
from pyspark.sql import SparkSession
class SourceAdapter:
    def read(self, spark, config):
        """
        Read data from a source using Spark session and source-specific configurations.
        :param spark: Spark session
        :param config: Configuration dictionary for the source
        """
        raise NotImplementedError("Subclasses must implement this method.")

class SinkAdapter:
    def write(self, df, config):
        """
        Write data to a sink using the provided DataFrame and sink-specific configurations.
        :param df: DataFrame to write
        :param config: Configuration dictionary for the sink
        """
        raise NotImplementedError("Subclasses must implement this method.")

    def get_latest_timestamp(self, spark: SparkSession) -> str:
        """
        # This method is intended to return the latest timestamp from the data in the sink.
        # It takes a SparkSession as input and returns a string representation of the latest timestamp.
        # This can be useful for incremental data loads where only new data since the last timestamp
        needs to be processed.
        """
        raise NotImplementedError("Subclasses must implement this method.")