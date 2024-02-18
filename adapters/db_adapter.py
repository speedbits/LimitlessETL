# db_adapter.py

from adapters.base_adapter import SourceAdapter, SinkAdapter
from pyspark.sql import DataFrameWriter


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
        df.writer = df_writer \
            .mode("overwrite") \
            .option("url", config['url']) \
            .option("dbtable", config['table']) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .save()
