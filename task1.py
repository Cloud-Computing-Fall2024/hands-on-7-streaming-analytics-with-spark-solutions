from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.appName("Task1_FilteredStream").getOrCreate()

# Set output paths
task1_output = "output/task1_filtered_stream.csv"
checkpoint_dir = "checkpoint/task1/"

# Read streaming data from the socket
stock_stream = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()

# Split the raw text data into columns
stock_stream_split = stock_stream.selectExpr("split(value, ',')[0] as Timestamp", 
                                             "split(value, ',')[1] as StockSymbol", 
                                             "cast(split(value, ',')[2] as float) as Price")

# Filter for stock prices greater than 100
filtered_stream = stock_stream_split.filter(col("Price") > 100)

# Write the filtered stream to CSV (Append mode)
filtered_stream.writeStream.format("csv")\
    .option("path", task1_output)\
    .option("header", True)\
    .option("checkpointLocation", checkpoint_dir)\
    .outputMode("append").start()\
    .awaitTermination()

print(f"Task 1 output written to {task1_output}")
