# file_adapter.py
import json

from adapters.base_adapter import SourceAdapter, SinkAdapter

class S3SourceAdapter(SourceAdapter):
    def read(self, spark, config):
        print(config['options'] )

        # options = json.loads(config['options'])
        # Initialize DataFrame reader with format, here assuming CSV
        df_reader = None

            # Load the DataFrame
        return df_reader.load(config['path'])

class S3SinkAdapter(SinkAdapter):
    def write(self, df, config):
        df_writer = df.write

        df_writer.save(config['path'])
