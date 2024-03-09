from pyspark.sql import SparkSession

from connectors.base_connector import BaseConnector


class DatabaseConnector(BaseConnector):
    def read(self, spark, config):
        df_reader = spark.read.format(config['format'])
        # Apply options from the dictionary
        if config.get('options') != None:
            for option, value in config['options'].items():
                print(option + " => " + value)
                df_reader = df_reader.option(option, value)

        return df_reader \
            .option("url", config['url']) \
            .option("dbtable", config['table']) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .load()

    def write(self, df, config):
        df_writer = df.write.format(config['format'])
        if config.get('options') != None:
            for option, value in config['options'].items():
                print(option + " => " + value)
                df_writer = df_writer.option(option, value)
        df.writer = df_writer \
            .mode("overwrite") \
            .option("url", config['url']) \
            .option("dbtable", config['table']) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .save()

    # TODO: make sure the timestamp column and SQL itself is externalized
    def get_latest_timestamp(self, spark: SparkSession, config: dict) -> str:
        table_name = config['table']
        timestamp_col = config['ts_column']
        query = f"(SELECT MAX({timestamp_col}) as max_timestamp FROM {table_name}) as subquery"
        # overriding table value with the query to get the dataframe with max timestamp
        config['table'] = query
        df = self.read(spark, config)
        latest_timestamp = df.collect()[0][0]
        return latest_timestamp
