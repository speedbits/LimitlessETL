from pyspark.sql import SparkSession
import boto3
import os

file_path = "D:/projects/LimitlessETL/data/cars_data.csv"
# Set AWS credentials (replace with your actual credentials)
os.environ["AWS_ACCESS_KEY_ID"] = "xxx"
os.environ["AWS_SECRET_ACCESS_KEY"] = "xxxx"

# Create SparkSession
spark = SparkSession.builder \
    .appName("DataFrameToS3") \
    .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"]) \
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"]) \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0,com.amazonaws:aws-java-sdk-bundle:1.11.375") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

#    .config("spark.jars", "/Users/akshayr/Desktop/APPS/spark-3.5.1-bin-hadoop3/jars/hadoop-aws-3.2.0.jar,"
#                          "/Users/akshayr/Desktop/APPS/spark-3.5.1-bin-hadoop3/jars/aws-java-sdk-bundle-1.11.271.jar") \
# Load data from CSV file to data frame
df = spark.read.csv(file_path, header=True, inferSchema=True)

# Count rows to check the data
row_count = df.count()
print("Number of rows in DataFrame:", row_count)

# Define S3 bucket and key
s3_bucket = 's3dataengg'
s3_key = 'project1/Users.csv'

# Write DataFrame to CSV format
csv_temp_path = "D:\\projects\\LimitlessETL\\data\\tmp/Users.csv"  # Temporary local path
df.write.csv(csv_temp_path, header=True, mode="overwrite")

# Upload CSV file to S3
s3 = boto3.client('s3')
for file in os.listdir(csv_temp_path):
    s3.upload_file(os.path.join(csv_temp_path, file), s3_bucket, f"{s3_key}/{file}")

print("DataFrame has been successfully written to S3 bucket.")

# Stop Spark session
spark.stop()