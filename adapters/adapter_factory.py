# adapter_factory.py

from adapters.file_adapter import FileSourceAdapter, FileSinkAdapter
from adapters.db_adapter import DatabaseSourceAdapter, DatabaseSinkAdapter
from adapters.s3_adapter import S3SinkAdapter, S3SourceAdapter

def get_adapter(adapter_type, io_type):
    if io_type == "source":
        if adapter_type == "file":
            return FileSourceAdapter()
        elif adapter_type == "database":
            return DatabaseSourceAdapter()
        elif adapter_type == "s3":
            return S3SourceAdapter()
        # Add more source types as needed
    elif io_type == "sink":
        if adapter_type == "file":
            return FileSinkAdapter()
        elif adapter_type == "database":
            return DatabaseSinkAdapter()
        elif adapter_type == "s3":
            return S3SinkAdapter()
        # Add more sink types as needed
    else:
        raise ValueError("Invalid IO type specified.")


# Example usage
if __name__ == "__main__":
    get_adapter("file", "source")
