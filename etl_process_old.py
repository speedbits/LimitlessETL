# etl_process_old.py

from pyspark.sql import SparkSession
from adapters.adapter_factory import get_adapter


def run_etl(spark, source_config, sink_config, transformations=None):
    source_adapter = get_adapter(source_config['type'], "source")
    sink_adapter = get_adapter(sink_config['type'], "sink")

    df = source_adapter.read(spark, source_config)

    # Apply transformations if any
    if transformations:
        for transformation in transformations:
            df = transformation(df)

    sink_adapter.write(df, sink_config)


if __name__ == "__main__":
    spark = SparkSession.builder.appName("ETL Framework").getOrCreate()

    # Example configurations
    source_config = {
        "type": "file",
        "format": "csv",
        "path": "path/to/source/data.csv"
    }

    sink_config = {
        "type": "database",
        "url": "jdbc:postgresql://localhost:5432/database",
        "table": "destination_table",
        "user": "username",
        "password": "password"
    }

    run_etl(spark, source_config, sink_config)
