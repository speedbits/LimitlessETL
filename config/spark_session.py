from pyspark.sql import SparkSession

# Key Points:
# _instance: A class variable that holds the single instance of the Spark session. Initially,
# it is set to None.
# get_instance: A class method that checks if the _instance variable is None.
# If it is, a new Spark session is created using SparkSession.builder, and _instance
# is set to this new session. If _instance is not None (meaning a session has already been created),
# the existing session is returned. This ensures that only one Spark session is created and used
# throughout the application.
# Singleton Implementation: This implementation uses a class method and a class variable to enforce
# the singleton pattern, ensuring that only one instance of the SparkSession is ever created within
# the application.

# Notes:
# The SparkSession.builder line can be customized with additional configurations specific to your
# Spark setup. For example, you might want to set the master URL, enable Hive support, or add other
# configuration options relevant to your environment or application needs.
# The Singleton pattern is useful in scenarios where exactly one object is needed to coordinate
# actions across the system. In the case of Spark applications, having a single Spark session is
# often a best practice to manage resources efficiently and avoid issues related to having multiple
# concurrent sessions.

class SparkSessionSingleton:
    """A Singleton class to manage the Spark session."""
    _instance = None

    @classmethod
    def get_instance(cls):
        """Method to get the single instance of the Spark session."""
        if cls._instance is None:
            cls._instance = SparkSession.builder \
                .appName("ETL Framework") \
                .config("spark.some.config.option", "some-value") \
                .config("spark.jars", "./jars/mysql-connector-j-8.3.0.jar") \
                .getOrCreate()
            print("New Spark session created.")
        else:
            print("Using existing Spark session.")
        return cls._instance


# Example usage
if __name__ == "__main__":
    # This will create a new Spark session
    spark = SparkSessionSingleton.get_instance()

    # This will reuse the existing Spark session
    spark2 = SparkSessionSingleton.get_instance()

    # Both 'spark' and 'spark2' variables will point to the same Spark session object
    assert spark == spark2
    print("Both spark variables point to the same Spark session.")
