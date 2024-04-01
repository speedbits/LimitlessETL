from pyspark.sql import SparkSession
from config.config_loader import  ETLConfigLoader
from adapters.adapter_factory import get_adapter
from adapters.file_adapter import FileSourceAdapter, FileSinkAdapter
from config.spark_session import SparkSessionSingleton
import logging
from etl_process import run_etl

if __name__ == "__main__":
    spark = SparkSessionSingleton.get_instance()
    config_path = "resources/flows/sample-data-flow-s3.json"
    run_etl(config_path, spark)