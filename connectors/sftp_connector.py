from connectors.base_connector import BaseConnector
import paramiko
from pyspark.sql import SparkSession

class SFTPConnector(BaseConnector):
    def read(self, spark, config):
        transport = paramiko.Transport((config["host"], config["port"]))
        transport.connect(username=config["username"], password=config["password"])
        sftp = paramiko.SFTPClient.from_transport(transport)

        # Assuming the file is downloaded to a local path before loading
        local_path = "/tmp/temp_file"
        sftp.get(config["remote_path"], local_path)
        sftp.close()
        transport.close()

        # Read the downloaded file with Spark
        return spark.read.format(config.get("format", "csv")) \
            .options(**config.get("options", {})) \
            .load(local_path)
