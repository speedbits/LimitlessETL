from connectors.base_connector import BaseConnector

class FileConnector(BaseConnector):
    def read(self, spark, config):
        print(config['options'] )

        # options = json.loads(config['options'])
        # Initialize DataFrame reader with format, here assuming CSV
        df_reader = spark.read.format(config['format'])
        # Apply options from the dictionary
        for option, value in config['options'].items():
            print(option + " => " + value)
            df_reader = df_reader.option(option, value)
            # Load the DataFrame
        return df_reader.load(config['path'])

    def write(self, df, config):
        df.write.format(config['format']).save(config['path'])