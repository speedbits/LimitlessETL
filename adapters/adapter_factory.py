# adapter_factory.py

from adapters.file_adapter import FileSourceAdapter, FileSinkAdapter
from adapters.db_adapter import DatabaseSourceAdapter, DatabaseSinkAdapter

def get_adapter(adapter_type, io_type):
    if io_type == "source":
        if adapter_type == "file":
            return FileSourceAdapter()
        elif adapter_type == "database":
            return DatabaseSourceAdapter()
        # Add more source types as needed
    elif io_type == "sink":
        if adapter_type == "file":
            return FileSinkAdapter()
        elif adapter_type == "database":
            return DatabaseSinkAdapter()
        # Add more sink types as needed
    else:
        raise ValueError("Invalid IO type specified.")


# Example usage
if __name__ == "__main__":
    get_adapter("file", "source")
