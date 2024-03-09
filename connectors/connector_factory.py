from connectors.file_connector import FileConnector
from connectors.database_connector import DatabaseConnector
from connectors.sftp_connector import SFTPConnector
from connectors.api_connector import APIConnector

class ConnectorFactory:
    @staticmethod
    def get_connector(connector_type):
        connectors = {
            "file": FileConnector,
            "sftp": SFTPConnector,
            "database": DatabaseConnector,
            "api": APIConnector
        }

        if connector_type in connectors:
            return connectors[connector_type]()
        else:
            raise ValueError(f"Connector type '{connector_type}' is not supported.")
