from pyspark.sql import SparkSession
# creating spark session
# spark = SparkSession.builder.appName("HelloWorld").getOrCreate()
spark = SparkSession.builder.master("local[1]") \
 .appName('SparkByExamples.com') \
 .getOrCreate()
sc = spark.sparkContext

nums = sc.parallelize([1,2,3,4])
print(nums)
