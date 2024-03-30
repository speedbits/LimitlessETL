from pyspark.sql import SparkSession
from config.config_loader import  ETLConfigLoader
from adapters.adapter_factory import get_adapter
from adapters.file_adapter import FileSourceAdapter, FileSinkAdapter
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
        print("Reading data from source...")
        df = source_adapter.read(spark, source_config)
        if df is None:
            raise ValueError("Failed to read data from source")
        df.show()

        print("Number of partitions after reading from source => "+str(df.rdd.getNumPartitions()))

        # Apply transformations
        if transformations:
            print("Applying transformations on the extracted data from source...")
            df = apply_transformations(df, transformations, spark)
            if df is None:
                raise ValueError("Failed to apply transformations")
            df.show()
        print("Number of partitions after transformation => " + str(df.rdd.getNumPartitions()))
        # Write data
        print("Writing data to sink...")
        print("sink_config[partition_count] => "+str(sink_config.get("partition_count")))
        if sink_config.get("partition_count") is not None:
            df = df.repartition(sink_config.get("partition_count"))
        print("Number of partitions before writing to sink  => " + str(df.rdd.getNumPartitions()))
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
        print(data_flows)
        # Sort data flows by sequence number
        sorted_data_flows = sorted(data_flows, key=lambda x: x.get("sequence", 0))
        dependency_job_status_map = {}

        for entry in sorted_data_flows:
            print("Entry => "+str(entry))
            dependency = entry.get("dependency")
            print("dependency => "+str(dependency))
            print(" dependency_job_status_map => "+str(dependency_job_status_map))
            # Check if dependency is met and then run
            if dependency is None:
                print("No dependency found, running the sequence => "+str(entry.get("sequence")))
                status = run_data_flow_entry(spark, entry)
                print("Status of sequence -> "+str(status))
                dependency_job_status_map[entry.get("sequence")] = status
            elif dependency is not None and dependency_job_status_map[dependency]:
                status = run_data_flow_entry(spark, entry)
                print("Status of sequence with dependency -> " + str(status))
                dependency_job_status_map[entry.get("sequence")] = status
            else:
                print("Dependency found, and seems like the dependent sequence failed to run successfully. Aborting sequence => "+str(entry.get("sequence")))
            # if dependency is not None and not run_data_flow_entry(spark,
            #                                                       config_loader.get_data_flow_by_sequence(dependency)):
            #     logger.info(f"Skipping data flow {entry.get('sequence')} due to unmet dependency.")
            #     continue

            # Execute the data flow
            #print("run_data_flow_entry for sequence => " + str(entry))
            #run_data_flow_entry(spark, entry)
    except Exception as e:
        logger.error(f"ETL process failed: {e}")


if __name__ == "__main__":
    spark = SparkSessionSingleton.get_instance()
    config_path = "resources/flows/sample-data-flow.json"
    run_etl(config_path, spark)