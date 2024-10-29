from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.appName("Task2_CurrencyConversion").getOrCreate()

# Set output paths
task2_output = "output/task2_currency_conversion.csv"
checkpoint_dir = "checkpoint/task2/"

# Read streaming data from the socket
stock_stream = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()

# Split the raw text data into columns
stock_stream_split = stock_stream.selectExpr("split(value, ',')[0] as Timestamp", 
                                             "split(value, ',')[1] as StockSymbol", 
                                             "cast(split(value, ',')[2] as float) as Price")

# Convert the price to EUR
conversion_rate = 0.85
stock_stream_converted = stock_stream_split.withColumn("PriceEUR", col("Price") * conversion_rate)

# Write the converted prices to CSV
stock_stream_converted.writeStream.format("csv")\
    .option("path", task2_output)\
    .option("header", True)\
    .option("checkpointLocation", checkpoint_dir)\
    .outputMode("append").start()\
    .awaitTermination()

print(f"Task 2 output written to {task2_output}")
