from pyspark.sql import SparkSession
from connectors.file_connector import FileConnector
from connectors.database_connector import DatabaseConnector
from connectors.sftp_connector import SFTPConnector
from connectors.api_connector import APIConnector
from connectors.connector_factory import ConnectorFactory

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Connector Framework").getOrCreate()

    # Example configurations for different source types
    configs = {
        "file": {"type": "file", "path": "path/to/your/file.csv", "format": "csv", "options": {"header": "true"}},
        "sftp": {"type": "sftp", "host": "example.com", "port": 22, "username": "user", "password": "pass", "remote_path": "/path/to/sftp/file.csv"},
        "database": {"type": "database", "url": "jdbc:postgresql://localhost:5432/database", "table": "your_table", "user": "user", "password": "pass"},
        "api": {"type": "api", "url": "https://api.example.com/data", "params": {"key": "value"}}
    }

    # Iterate over the configs and use the factory to get the appropriate connector and read data
    for config_type, config in configs.items():
        print(f"Testing {config_type} connector")
        connector = ConnectorFactory.get_connector(config_type)
        df = connector.read(spark, config)
        df.show()

    spark.stop()
