import socket
import time
import random
from datetime import datetime

# List of stock symbols representing more companies
stock_symbols = [
    "AAPL", "GOOGL", "AMZN", "TSLA", "MSFT", "NFLX", 
    "FB", "BABA", "NVDA", "INTC", "ORCL", "IBM", 
    "TWTR", "DIS", "V", "PYPL", "CSCO", "QCOM", 
    "ADBE", "SAP", "CRM", "UBER", "LYFT", "SHOP", 
    "PINS", "SNAP", "SQ", "PLTR", "ZM", "NIO"
]

def generate_stock_price():
    """ Function to generate stock price data. """
    while True:
        symbol = random.choice(stock_symbols)
        price = round(random.uniform(80, 2000), 2)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        message = f"{timestamp},{symbol},{price}\n"
        yield message
        time.sleep(1)

def start_socket_server(host="localhost", port=9999):
    """ Function to start a socket server to stream data. """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.bind((host, port))
        server_socket.listen(1)  # Listen for a connection
        print(f"Server started at {host}:{port}. Waiting for client connection...")

        conn, addr = server_socket.accept()
        print(f"Connected by {addr}")

        with conn:
            for stock_data in generate_stock_price():
                conn.sendall(stock_data.encode())  # Send stock price data to client
                print(f"Sent: {stock_data.strip()}")

if __name__ == "__main__":
    start_socket_server()
