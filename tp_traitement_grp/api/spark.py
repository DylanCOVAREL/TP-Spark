from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Climate API") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
