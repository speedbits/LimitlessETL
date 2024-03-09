from connectors.base_connector import BaseConnector

import requests
from pyspark.sql import SparkSession


class APIConnector(BaseConnector):
    def read(self, spark, config):
        response = requests.get(config["url"], params=config.get("params"))
        data = response.json()  # Assuming JSON response. Adjust as needed.

        # Convert the JSON response to a DataFrame. This example assumes a simple flat structure.
        return spark.createDataFrame(data)
