from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

spark = SparkSession.builder \
    .appName("Windy Streaming Weather") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Lecture streaming des fichiers JSON
df_stream = spark.readStream \
    .option("multiline", "true") \
    .json("data/stream/windy")

df_processed = df_stream \
    .withColumn("ingestion_time", current_timestamp())

query = df_processed.writeStream \
    .format("parquet") \
    .option("path", "data/streaming_output/weather") \
    .option("checkpointLocation", "data/streaming_output/checkpoint") \
    .outputMode("append") \
    .start()

query.awaitTermination()
