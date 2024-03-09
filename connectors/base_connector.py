from pyspark.sql import SparkSession


class BaseConnector:
    def read(self, spark, config):
        raise NotImplementedError("Subclasses must implement this method.")

    def write(self, spark, config):
        raise NotImplementedError("Subclasses must implement this method.")

    def get_latest_timestamp(self, spark: SparkSession, config: dict):
        raise NotImplementedError("Subclasses must implement this method.")