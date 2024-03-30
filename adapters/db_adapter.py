# db_adapter.py
from adapters.base_adapter import SourceAdapter, SinkAdapter
from pyspark.sql import DataFrameWriter, SparkSession


class DatabaseSourceAdapter(SourceAdapter):
    def read(self, spark, config):
        df_reader = spark.read.format(config['format'])
        # Apply options from the dictionary
        for option, value in config['options'].items():
            print(option + " => " + value)
            df_reader = df_reader.option(option, value)
        return df_reader \
            .option("url", config['url']) \
            .option("dbtable", config['table']) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .load()

class DatabaseSinkAdapter(SinkAdapter):
    def write(self, df, config):
        df_writer = df.write.format(config['format'])
        for option, value in config['options'].items():
            print(option + " => " + value)
            df_writer = df_writer.option(option, value)
        if config['mode'] is not None:
            df_writer.mode(config['mode'])
        df.writer = df_writer \
            .option("url", config['url']) \
            .option("dbtable", config['table']) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .save()

    # TODO: make sure the timestamp column and SQL itself is externalized
    def get_latest_timestamp(self, spark: SparkSession, config: dict) -> str:
        query = f"(SELECT MAX(timestamp) as max_timestamp FROM {self.table_name}) as subquery"

        df = spark.read.format("jdbc").option("url", self.jdbc_url).option("dbtable", query).option("driver",
                                                                                                    "com.mysql.cj.jdbc.Driver").load()
        latest_timestamp = df.collect()[0][0]
        return latest_timestamp