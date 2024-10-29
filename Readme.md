### **Hands-on #7: Streaming Analytics with Spark**

#### **Objective:**
In this hands-on activity, you will learn the fundamentals of **Spark Streaming** by processing a stream of real-time data. The objective is to get hands-on experience in setting up a streaming source, applying basic transformations, and performing analysis on the live data stream. The data stream will simulate a stream of stock prices, and you will perform basic calculations like moving averages and detecting large price changes.

---

### **Dataset:**
The dataset consists of a simulated stream of stock prices, generated in real-time. Each record in the stream contains the following information:

- **Timestamp**: The time at which the stock price was recorded.
- **StockSymbol**: The symbol of the stock (e.g., AAPL, TSLA, MSFT).
- **Price**: The stock price at that moment.

---

### **Setup and Execution Instructions:**

#### **Step 1: Fork the Repository**
1. Fork the GitHub repository containing the starter code Using the Github Classroom link posted on Canvas.
2. Clone the forked repository to your GitHub account.

#### **Step 2: Launch GitHub Codespaces**
1. Open your forked repository in **GitHub Codespaces**.
2. Ensure that the GitHub Codespace has **Java** and **Python** installed (both are installed by default in GitHub Codespaces).

#### **Step 3: Install PySpark**
The activity requires **PySpark** to run Spark Streaming jobs. Install it using `pip`.

In the GitHub Codespace terminal, run the following command:
```bash
pip install pyspark
```

---

### **Tasks:**

#### **Task 1: Real-Time Data Ingestion**
- **Objective**: Set up a **data stream** to ingest real-time stock price data.
- **Instructions**:
    1. Use a **socket** as the data source to simulate a stream of stock prices. 
    2. Connect to a socket at `localhost:9999`.
    3. Use the data format:
        - Timestamp, StockSymbol, Price (e.g., `2023-10-30 10:15:30, AAPL, 150.5`)
    4. Continuously ingest data from the stream using Spark’s `readStream` API.

**Expected Outcome**: Spark will continuously read stock price data from the socket.

---

#### **Task 2: Basic Transformations**
- **Objective**: Perform basic transformations on the streaming data.
- **Instructions**:
    1. Parse the incoming data to extract the timestamp, stock symbol, and price.
    2. Convert the stock price to a floating-point number.
    3. Filter out records with missing or incorrect data.
    4. Group the stock prices by `StockSymbol`.

**Expected Outcome**: A DataFrame containing the parsed stock prices, grouped by stock symbol.

---

#### **Task 3: Moving Average Calculation**
- **Objective**: Calculate a **moving average** of the stock price for each stock symbol.
- **Instructions**:
    1. Calculate the moving average for each stock symbol over the last 10 seconds of data.
    2. Use a **sliding window** of 10 seconds to compute the average stock price.

**Expected Outcome**: A DataFrame showing the stock symbol and its moving average price over the last 10 seconds.

---

#### **Task 4: Detect Large Price Changes**
- **Objective**: Detect when a stock’s price changes by more than a certain threshold within a short time period.
- **Instructions**:
    1. Track the price of each stock symbol over time.
    2. Detect when the price of a stock changes by more than 5% compared to the previous price.
    3. Output an alert whenever a large price change is detected.

**Expected Outcome**: A DataFrame showing stock symbols that experienced a large price change, along with the percentage change.

---

### **Step 4: Simulating the Data Stream**

1. You will simulate a real-time data stream using **Netcat** or another utility to generate stock price data.
2. To start a stream, run the following command in a separate terminal:
```bash
nc -lk 9999
```
3. In the Netcat terminal, manually enter stock price data in the following format:
```
2023-10-30 10:15:30, AAPL, 150.5
2023-10-30 10:16:30, TSLA, 900.0
```

You can continuously enter or script the input to simulate a stream of stock prices.

---

### **Step 5: Run the Streaming Job Using `spark-submit`**

After implementing the tasks, execute the Spark Streaming job using `spark-submit`:

```bash
spark-submit <your_python_script.py>
```
Replace `<your_python_script.py>` with the name of your Python script.

---

### **Step 6: Submit Your Work**

Once you have completed the tasks:
1. Commit and push your code to your GitHub repository.
2. Ensure that all the task logic is implemented correctly in the Python script.
3. Submit the repository link on GitHub Classroom.

---

### **Expected Output:**

- **Task 1**: Continuous stream of stock price data ingested from the socket.
- **Task 2**: Parsed and grouped stock price data.
- **Task 3**: Moving average of stock prices for each stock symbol.
- **Task 4**: Alerts for large stock price changes.

---

### **Grading Criteria:**

- **Correctness**: Ensure the data is ingested correctly from the stream and transformations are applied as per the task requirements.
- **Execution**: Ensure the streaming job runs without errors and continuously processes incoming data.
- **Output**: Verify that the output matches the expected results for each task.

---

### **Support and Questions:**

If you have any questions or run into issues, feel free to reach out during office hours or post your queries in the course discussion forum.

---