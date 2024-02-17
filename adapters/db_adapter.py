# db_adapter.py

from base_adapter import SourceAdapter, SinkAdapter
from pyspark.sql import DataFrameWriter


class DatabaseSourceAdapter(SourceAdapter):
    def read(self, spark, config):
        return spark.read \
            .format("jdbc") \
            .option("url", config['url']) \
            .option("dbtable", config['table']) \
            .option("user", config['user']) \
            .option("password", config['password']) \
            .load()

class DatabaseSinkAdapter(SinkAdapter):
    def write(self, df, config):
        df.write \
            .format("jdbc") \
            .option("url", config['url']) \
            .option("dbtable", config['table']) \
            .option("user", config['user']) \
            .option("password", config['password']) \
            .save()
