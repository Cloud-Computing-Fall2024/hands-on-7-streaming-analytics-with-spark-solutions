# **Hands-on #7: Streaming Analytics with Spark**

## **Objective**

In this hands-on activity, you will learn how to process real-time data streams using **Spark Streaming**. You will perform various transformations and analyses on stock price data streamed in real-time from a Python socket server.

---

## **Data Generation**

The stock price data will be generated in real-time by a Python script, which simulates stock prices for multiple companies. The data will be streamed through a socket connection to Spark.

### **Steps to Set Up the Data Stream**

1. **Run the Data Generation Script**: This script will simulate stock price data and stream it through a socket on `localhost:9999`.
2. **Connect Spark Streaming**: Your Spark job will connect to `localhost:9999` to receive and process the data stream.

### **Data Generation Script**

Use the following Python script to generate stock price data:

```python
import socket
import time
import random
from datetime import datetime

# List of stock symbols representing various companies
stock_symbols = [
    "AAPL", "GOOGL", "AMZN", "TSLA", "MSFT", "NFLX", 
    "FB", "BABA", "NVDA", "INTC", "ORCL", "IBM", 
    "TWTR", "DIS", "V", "PYPL", "CSCO", "QCOM", 
    "ADBE", "SAP", "CRM", "UBER", "LYFT", "SHOP", 
    "PINS", "SNAP", "SQ", "PLTR", "ZM", "NIO"
]

def generate_stock_price():
    while True:
        symbol = random.choice(stock_symbols)
        price = round(random.uniform(80, 2000), 2)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        message = f"{timestamp},{symbol},{price}\n"
        yield message
        time.sleep(1)

def start_socket_server(host="localhost", port=9999):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.bind((host, port))
        server_socket.listen(1)
        print(f"Server started at {host}:{port}. Waiting for client connection...")

        conn, addr = server_socket.accept()
        print(f"Connected by {addr}")

        with conn:
            for stock_data in generate_stock_price():
                conn.sendall(stock_data.encode())
                print(f"Sent: {stock_data.strip()}")

if __name__ == "__main__":
    start_socket_server()
```

### **How to Run the Data Generation Script**

1. Run the Python script to start the socket server:
   ```bash
   python generate_stock_price.py
   ```
   The script will stream real-time stock price data to `localhost:9999`.

2. **Connect Spark**: Your Spark job should connect to the socket using:
   ```python
   stock_stream = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()
   ```

---

## **Tasks**

You are required to implement the following tasks using Spark Streaming and Spark SQL:

### **Task 1: Real-Time Data Ingestion with Filtering**

- **Objective**: Ingest real-time stock price data and filter out any prices outside the range of $100 to $2000.
- **Instructions**: Implement logic to filter the stock prices and write the filtered data to a CSV file.

### **Task 2: Basic Transformations and Currency Conversion**

- **Objective**: Convert stock prices from USD to EUR using a conversion rate of `1 USD = 0.85 EUR`.
- **Instructions**: Add a new column with the converted prices and write the transformed data to a CSV file.

### **Task 3: Moving Average with Dynamic Time Windows**

- **Objective**: Calculate the moving average of the stock prices for each stock symbol over a window of 15 seconds, sliding by 5 seconds.
- **Instructions**: Use window functions to calculate the moving average and write the result to a CSV file.

### **Task 4: Detect Sudden Price Spikes or Drops**

- **Objective**: Detect when the stock price changes by more than 10% compared to the previous price.
- **Instructions**: Track the previous price using a window function, calculate the price change, and write the result to a CSV file when the change exceeds 10%.

---

## **Execution Instructions**

1. **Install PySpark**: If PySpark is not already installed, install it using `pip`:
   ```bash
   pip install pyspark
   ```

2. **Run the Data Generation Script**: Start the socket server to stream stock price data:
   ```bash
   python generate_stock_price.py
   ```

3. **Submit the Spark Job**: Use the `spark-submit` command to run your Spark job:
   ```bash
   spark-submit streaming_job.py
   ```

4. **CSV Output**: Ensure that each task writes its output to a separate CSV file in the `output/` directory.

---

## **GitHub Submission Instructions**

### **Step 1: Fork the GitHub Repository**
1. **Click the GitHub Classroom link** provided in Canvas.
2. Fork the repository to your own GitHub account.
3. Clone the repository to your local machine or use **GitHub Codespaces** for development.

### **Step 2: Complete the Activity**
1. Write your Spark Streaming logic for each task in the appropriate function in the provided Python script.
2. Ensure that each task writes its results to a separate CSV file in the `output/` directory.

### **Step 3: Push Your Changes to GitHub**
1. After completing the tasks, add the files to Git:
   ```bash
   git add .
   ```
2. Commit your changes with a meaningful message:
   ```bash
   git commit -m "Completed Spark Streaming Hands-on Activity"
   ```
3. Push the changes to your repository:
   ```bash
   git push origin main
   ```

### **Step 4: Submit the GitHub Repository**
1. Submit the link to your **GitHub repository** in GitHub Classroom.

---

## **Deliverables**

1. **Source Code**: Submit the source code for each task in your Spark Streaming job.
2. **CSV Outputs**: Submit the CSV files generated by each task, saved in the `output/` directory.

---

## **Grading Criteria**

- **Correctness**: Each task should correctly implement the required logic and output the correct results.
- **Use of Spark SQL**: Demonstrate proper use of Spark SQL transformations and window functions.
- **Output**: Each task should write the expected results to a separate CSV file.
- **Submission**: Ensure all files (CSV outputs and code) are pushed to GitHub and the repository link is submitted via GitHub Classroom.

---