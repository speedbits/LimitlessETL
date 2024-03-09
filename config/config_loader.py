import json

# How This Works:
# The ETLConfigLoader class is initialized with the path to a JSON configuration file.
# The _load_config private method attempts to open and parse the JSON file, storing its content
# in the config attribute.
# The config dictionary is expected to have a key "data_flows" that contains a list of data flow entries.
# These entries are made directly accessible through the data_flows attribute.
# The get_data_flows method returns the list of all data flow entries.
# The get_data_flow_by_sequence method allows retrieving a specific data flow entry by its sequence number.

# Usage:
# Instantiate the ETLConfigLoader with the path to your JSON configuration file. Then, use its methods
# to access the entire list of data flows or specific entries as needed for your ETL processes.
#
# This class simplifies the process of working with the JSON configuration, abstracting away file handling
# and parsing logic. It allows the rest of your ETL framework to focus on processing data flows rather
# than dealing with configuration loading intricacies.
import os


class ETLConfigLoader:
    def __init__(self, filepath):
        self.filepath = filepath
        self.config = self._load_config()
        self.data_flows = self.config.get("data_flows", [])

    def _load_config(self):
        """
        Loads the JSON configuration from the specified file.
        """
        file_dir = os.path.dirname(os.path.realpath('__file__'))
        print("file_dir => "+file_dir)
        try:
            with open(self.filepath, 'r') as file:
                config = json.load(file)
                return config
        except FileNotFoundError:
            print(f"File {self.filepath} not found.")
            return {}
        except json.JSONDecodeError:
            print(f"Error decoding JSON from {self.filepath}.")
            return {}

    def get_data_flows(self):
        """
        Returns a list of data flow entries from the configuration.
        """
        return self.data_flows

    def get_data_flow_by_sequence(self, sequence):
        """
        Returns a single data flow entry matching the given sequence number.
        """
        for data_flow in self.data_flows:
            if data_flow.get("sequence") == sequence:
                return data_flow
        return None


# Example usage
if __name__ == "__main__":
    # Currently relative file path does not work (e.g ./flows/sample-data-flow.json)
    config_loader = ETLConfigLoader("/resources/flows\\sample-data-flow.json")

    data_flows = config_loader.get_data_flows()
    if data_flows:
        print("Number of flows: "+str(len((data_flows))))

    # Access a specific data flow by sequence number
    data_flow = config_loader.get_data_flow_by_sequence(1)
    if data_flow:
        print(data_flow)
    else:
        print("Data flow not found.")
