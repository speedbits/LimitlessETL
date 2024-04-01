from pyspark.sql import SparkSession
import boto3
import os

# Install Python 3.12

# Set AWS credentials (replace with your actual credentials)
os.environ["AWS_ACCESS_KEY_ID"] = "xxx"
os.environ["AWS_SECRET_ACCESS_KEY"] = "xxxx"
print("Creating Spark Session...")
# Create a SparkSession
spark = SparkSession.builder \
    .appName("ReadFromS3") \
    .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"]) \
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"]) \
    .getOrCreate()
print("Spark Session Created.")
# Set up boto3 client
s3_client = boto3.client('s3')

# Define S3 bucket and key
s3_bucket = "s3dataengg"
s3_key = "project1/Users.csv"

# Download the file from S3
response = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
file_content = response['Body'].read().decode('utf-8')

print("Reading the file from S3 Bucket...")

# Convert the file content to RDD
lines_rdd = spark.sparkContext.parallelize(file_content.split('\n'))
print("Reading the file from S3 Bucket...2")
# Convert RDD to DataFrame
df = spark.read.csv(lines_rdd, header=True, inferSchema=True)
print("Writing the file to DataFrame...")
# Show the DataFrame schema
df.printSchema()

# Show some rows of the DataFrame
df.show()

csv_local_path = "D:\\projects\\LimitlessETL\\data\\tmp/Users1.csv"  # Temporary local path
df.write.csv(csv_local_path, header=True, mode="overwrite")

print("Data written to Local Disk at",{csv_local_path})

# Stop Spark session
spark.stop()