from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, max, min, expr, to_timestamp

# Initialize Spark session
spark = SparkSession.builder.appName("Task4_PriceSpikes").getOrCreate()

# Set output paths
task4_output = "output/task4_price_spike_alerts.csv"
checkpoint_dir = "checkpoint/task4/"

# Read streaming data from the socket
stock_stream = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()

# Split the raw text data into columns and cast Timestamp
stock_stream_with_watermark = stock_stream.selectExpr("split(value, ',')[0] as Timestamp", 
                                                      "split(value, ',')[1] as StockSymbol", 
                                                      "cast(split(value, ',')[2] as float) as Price") \
                                           .withColumn("Timestamp", to_timestamp(col("Timestamp"), "yyyy-MM-dd HH:mm:ss")) \
                                           .withWatermark("Timestamp", "1 minute")

# Calculate max and min prices over a window of 15 seconds, sliding every 5 seconds
price_extremes = stock_stream_with_watermark.groupBy(
    window(col("Timestamp"), "15 seconds", "5 seconds"), "StockSymbol"
).agg(max("Price").alias("MaxPrice"), min("Price").alias("MinPrice"))

# Detect spikes where price changes by more than 10%
price_spikes = price_extremes.filter(expr("MaxPrice > MinPrice * 1.1"))

# Flatten the window struct into two separate columns (start and end)
price_spikes_flattened = price_spikes.withColumn("WindowStart", col("window.start")) \
                                     .withColumn("WindowEnd", col("window.end")) \
                                     .drop("window")

# Write the detected spikes to CSV (using complete mode for aggregation)
price_spikes_flattened.writeStream.format("csv")\
    .option("path", task4_output)\
    .option("header", True)\
    .option("checkpointLocation", checkpoint_dir)\
    .outputMode("append").start()\
    .awaitTermination()

print(f"Task 4 output written to {task4_output}")
