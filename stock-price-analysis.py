from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, avg, lag, expr
from pyspark.sql.window import Window

# Create a Spark session
spark = SparkSession.builder.appName("AdvancedStreamingStockPrices").getOrCreate()

# Define schema of the incoming data (timestamp, stock symbol, price)
stock_schema = "Timestamp STRING, StockSymbol STRING, Price FLOAT"

# Output paths for each task
output_dir = "output/"
task1_output = output_dir + "task1_filtered_ingestion.csv"
task2_output = output_dir + "task2_currency_conversion.csv"
task3_output = output_dir + "task3_moving_average.csv"
task4_output = output_dir + "task4_price_spike_alerts.csv"

# ------------------------
# Read data from the socket
# ------------------------
# Reading the real-time data stream from the socket server
stock_stream = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()

# Split the input data into individual columns: Timestamp, StockSymbol, and Price
stock_stream = stock_stream.selectExpr("split(value, ',')[0] as Timestamp", 
                                       "split(value, ',')[1] as StockSymbol", 
                                       "cast(split(value, ',')[2] as float) as Price")


# ------------------------
# Task 1: Real-Time Data Ingestion with Filtering
# ------------------------
def task1_filtered_ingestion(stock_stream):
    filtered_stream = stock_stream.filter((col("Price") >= 100) & (col("Price") <= 2000))
    filtered_stream.writeStream.format("csv").option("path", task1_output).option("checkpointLocation", "checkpoint/task1").start()
    print(f"Task 1 output written to {task1_output}")

# ------------------------
# Task 2: Basic Transformations and Currency Conversion
# ------------------------
def task2_currency_conversion(stock_stream):
    transformed_stream = stock_stream.withColumn("PriceInEUR", col("Price") * 0.85)
    transformed_stream.writeStream.format("csv").option("path", task2_output).option("checkpointLocation", "checkpoint/task2").start()
    print(f"Task 2 output written to {task2_output}")

# ------------------------
# Task 3: Moving Average with Dynamic Time Windows
# ------------------------
def task3_moving_average(stock_stream):
    moving_avg = stock_stream.groupBy(window(col("Timestamp"), "15 seconds", "5 seconds"), "StockSymbol").agg(avg("Price").alias("MovingAvg"))
    moving_avg.writeStream.format("csv").option("path", task3_output).option("checkpointLocation", "checkpoint/task3").start()
    print(f"Task 3 output written to {task3_output}")

# ------------------------
# Task 4: Detect Sudden Price Spikes or Drops
# ------------------------
def task4_price_spikes(stock_stream):
    windowSpec = Window.partitionBy("StockSymbol").orderBy("Timestamp")
    prev_price = lag(col("Price"), 1).over(windowSpec)
    price_change = stock_stream.withColumn("PriceChange", (col("Price") - prev_price) / prev_price * 100)
    large_changes = price_change.filter(col("PriceChange") > 10)
    large_changes.writeStream.format("csv").option("path", task4_output).option("checkpointLocation", "checkpoint/task4").start()
    print(f"Task 4 output written to {task4_output}")

# Run the tasks
task1_filtered_ingestion(stock_stream)
task2_currency_conversion(stock_stream)
task3_moving_average(stock_stream)
task4_price_spikes(stock_stream)

# Stop the Spark session
spark.stop()
