# file_adapter.py

from base_adapter import SourceAdapter, SinkAdapter

class FileSourceAdapter(SourceAdapter):
    def read(self, spark, config):
        return spark.read.format(config['format']).load(config['path'])

class FileSinkAdapter(SinkAdapter):
    def write(self, df, config):
        df.write.format(config['format']).save(config['path'])
