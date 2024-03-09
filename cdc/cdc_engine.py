# Implement CDC engine
from pyspark.sql import SparkSession

from adapters.base_adapter import SourceAdapter, SinkAdapter


class CDCEngine:
    def __init__(self, source_adapter: SourceAdapter, sink_adapter: SinkAdapter):
        self.source_adapter = source_adapter
        self.sink_adapter = sink_adapter

    def execute(self, spark: SparkSession):
        latest_timestamp = self.sink_adapter.get_latest_timestamp(spark)
        source_df = self.source_adapter.read(spark)
        # Assume source data has a timestamp column for simplicity
        changes_df = source_df.filter(source_df.timestamp > latest_timestamp)
        self.sink_adapter.write(changes_df)