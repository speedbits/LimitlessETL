from pyspark.sql import SparkSession

from config.spark_session import SparkSessionSingleton
from connectors.file_connector import FileConnector
from connectors.database_connector import DatabaseConnector
from connectors.sftp_connector import SFTPConnector
from connectors.api_connector import APIConnector
import configparser

if __name__ == "__main__":
    # spark = SparkSession.builder.appName("Connector Framework").getOrCreate()
    spark = SparkSessionSingleton.get_instance()

    # # File Connector Test
    # file_config = {"path": "path/to/your/file.csv", "format": "csv", "options": {"header": "true"}}
    # file_df = FileConnector().read(file_config)
    # file_df.show()
    #
    # # SFTP Connector Test
    # # Ensure to fill in the SFTP details
    # sftp_config = {"host": "example.com", "port": 22, "username": "user", "password": "pass", "remote_path": "/path/to/sftp/file.csv"}
    # sftp_df = SFTPConnector().read(sftp_config)
    # sftp_df.show()

    # Database Connector Test
    # Ensure to fill in the database details
    src_db_config = {"url": "jdbc:mysql://localhost:3306/limitlessdb", "table": "cars", "format": "jdbc",
                     "options": {"user": "root", "password": "root"}}
    src_db_df = DatabaseConnector().read(spark, src_db_config)
    src_db_df.show()

    sink_db_config = {"url": "jdbc:mysql://localhost:3306/limitlessdb", "table": "cars_archive", "format": "jdbc",
                     "options": {"user": "root", "password": "root"}}
    DatabaseConnector().write(src_db_df, sink_db_config)
    sink_db_df = DatabaseConnector().read(spark, sink_db_config)
    sink_db_df.show()

    # # API Connector Test
    # api_config = {"url": "https://api.example.com/data", "params": {"key": "value"}}
    # api_df = APIConnector().read(api_config)
    # api_df.show()
