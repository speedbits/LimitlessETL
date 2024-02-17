# base_adapter.py

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
