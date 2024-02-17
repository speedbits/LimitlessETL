
Helps to track the number of records written
https://github.com/scravy/pysparkextra#sparkmetrics

If you are using DataFrames on Spark 3.3+, then the modern way to do this is with DataFrame.observe.
https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.observe.html
e.g:
https://stackoverflow.com/questions/68125593/pyspark-count-number-of-rows-written

Another option
df.first()['num_inserted_rows']
because pyspark.sql.dataframe.DataFrame = [num_affected_rows: long, num_inserted_rows: long]

df = spark.sql("""
    INSERT INTO tableB
    SELECT * FROM tableA LIMIT 1;
""")

or
df_history.select(F.col('operationMetrics')).collect()[0].operationMetrics['numOutputRows']