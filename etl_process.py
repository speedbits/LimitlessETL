from pyspark.sql import SparkSession
from config.config_loader import  ETLConfigLoader
from adapters.adapter_factory import get_adapter
from config.spark_session import SparkSessionSingleton
import logging

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def apply_transformations(df, transformations, spark):
    """
    Apply SQL transformations to the DataFrame.
    """
    try:
        for transformation in transformations:
            sql_expr = transformation.get("sql")
            df.createOrReplaceTempView("__THIS__")
            df = spark.sql(sql_expr)
        return df
    except Exception as e:
        logger.error(f"Error applying transformations: {e}")
        return None


def run_data_flow_entry(spark, entry):
    """
    Run a single data flow entry, including reading, transformations, and writing.
    """
    try:
        source_config = entry.get("source")
        sink_config = entry.get("sink")
        transformations = entry.get("transformations", [])

        # Get adapters based on the source and sink configurations
        source_adapter = get_adapter(source_config['type'], "source")
        sink_adapter = get_adapter(sink_config['type'], "sink")

        # Read data
        df = source_adapter.read(spark, source_config)
        if df is None:
            raise ValueError("Failed to read data from source")

        # Apply transformations
        if transformations:
            df = apply_transformations(df, transformations, spark)
            if df is None:
                raise ValueError("Failed to apply transformations")

        # Write data
        sink_adapter.write(df, sink_config)

        logger.info(f"Data flow {entry.get('sequence')} completed successfully.")
        return True
    except Exception as e:
        logger.error(f"Failed to complete data flow {entry.get('sequence')}: {e}")
        return False


def run_etl(config_path, spark):
    """
    Load the ETL configuration and run each data flow in sequence.
    """
    try:
        config_loader = ETLConfigLoader(config_path)
        data_flows = config_loader.get_data_flows()

        # Sort data flows by sequence number
        sorted_data_flows = sorted(data_flows, key=lambda x: x.get("sequence", 0))

        for entry in sorted_data_flows:
            dependency = entry.get("dependency")

            # Check if dependency is met
            if dependency is not None and not run_data_flow_entry(spark,
                                                                  config_loader.get_data_flow_by_sequence(dependency)):
                logger.info(f"Skipping data flow {entry.get('sequence')} due to unmet dependency.")
                continue

            # Execute the data flow
            run_data_flow_entry(spark, entry)
    except Exception as e:
        logger.error(f"ETL process failed: {e}")


if __name__ == "__main__":
    spark = SparkSessionSingleton.get_instance()
    config_path = "path/to/your/config.json"
    run_etl(config_path, spark)