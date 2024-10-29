from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, avg, to_timestamp

# Initialize Spark session
spark = SparkSession.builder.appName("Task3_MovingAverage").getOrCreate()

# Set output paths
task3_output = "output/task3_moving_average.csv"
checkpoint_dir = "checkpoint/task3/"

# Read streaming data from the socket
stock_stream = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()

# Split the raw text data into columns and cast Timestamp
stock_stream_with_watermark = stock_stream.selectExpr("split(value, ',')[0] as Timestamp", 
                                                      "split(value, ',')[1] as StockSymbol", 
                                                      "cast(split(value, ',')[2] as float) as Price") \
                                           .withColumn("Timestamp", to_timestamp(col("Timestamp"), "yyyy-MM-dd HH:mm:ss")) \
                                           .withWatermark("Timestamp", "1 minute")

# Calculate moving average with a window of 15 seconds, sliding every 5 seconds
moving_avg = stock_stream_with_watermark.groupBy(
    window(col("Timestamp"), "15 seconds", "5 seconds"), "StockSymbol"
).agg(avg("Price").alias("MovingAvg"))

# Flatten the window struct into two separate columns (start and end)
moving_avg_flattened = moving_avg.withColumn("WindowStart", col("window.start")) \
                                 .withColumn("WindowEnd", col("window.end")) \
                                 .drop("window")

# Write the moving average to CSV (using complete mode for aggregation)
moving_avg_flattened.writeStream.format("csv")\
    .option("path", task3_output)\
    .option("header", True)\
    .option("checkpointLocation", checkpoint_dir)\
    .outputMode("append").start()\
    .awaitTermination()

print(f"Task 3 output written to {task3_output}")
