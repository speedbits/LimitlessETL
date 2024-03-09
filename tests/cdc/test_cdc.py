# Usage
from pyspark.sql import SparkSession

from adapters.db_adapter import DatabaseSourceAdapter, DatabaseSinkAdapter
from cdc.cdc_engine import CDCEngine

if __name__ == "__main__":
    spark = SparkSession.builder.appName("CDCFramework").getOrCreate()

    source_adapter = DatabaseSourceAdapter(jdbc_url="jdbc:postgresql://localhost:5432/mydb", table_name="source_table")
    sink_adapter = DatabaseSinkAdapter(jdbc_url="jdbc:postgresql://localhost:5432/mydb", table_name="sink_table")

    cdc_engine = CDCEngine(source_adapter, sink_adapter)
    cdc_engine.execute(spark)

    spark.stop()