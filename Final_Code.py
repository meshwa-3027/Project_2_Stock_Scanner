from alice_credentials import login
from datetime import datetime
import json
import pandas as pd
import xlwings as xw

# create "alice" object using "alice_credentials" for login purpose
alice = login()

# Initiate Excel Workbook
wb = xw.Book("Stocks_Scanner.xlsx")
sht = wb.sheets['Sheet1']
sht.range('C1:J200').value = None

# Websocket connection to fetch LIVE data feed
LTP = 0
socket_opened = False
subscribe_flag = False
subscribe_list = []
unsubscribe_list = []
data = {}

def socket_open():
    print("Connected")
    global socket_opened
    socket_opened = True
    if subscribe_flag:
        alice.subscribe(subscribe_list)

def socket_close():
    global socket_opened, LTP
    socket_opened = False
    LTP = 0
    print("Closed")

def socket_error(message):
    global LTP
    LTP = 0
    print("Error :", message)

def feed_data(message):
    global LTP, subscribe_flag, data
    feed_message = json.loads(message)
    if feed_message["t"] == "ck":
        print("Connection Acknowledgement status :%s (Websocket Connected)" % feed_message["s"])
        subscribe_flag = True
        print("subscribe_flag :", subscribe_flag)
        print("-------------------------------------------------------------------------------")
        pass
    elif feed_message["t"] == "tk":
        token = feed_message["tk"]
        if "ts" in feed_message:
            symbol = feed_message["ts"]
        else:   
            symbol = token  # For indices
        data[symbol] = {
            "Open": feed_message.get("o", 0),
            "High": feed_message.get("h", 0),
            "Low": feed_message.get("l", 0),
            "LTP": feed_message.get("lp", 0),
            "OI": feed_message.get("toi", 0),
            "VWAP": feed_message.get("ap", 0),
            "PrevDayClose": feed_message.get("c", 0),
        }
        print(f"Token Acknowledgement status for {symbol}: {feed_message}")
        print("-------------------------------------------------------------------------------")
        pass
    else:
        print("Feed :", feed_message)
        LTP = feed_message["lp"] if "lp" in feed_message else LTP

alice.start_websocket(socket_open_callback=socket_open, socket_close_callback=socket_close,
                      socket_error_callback=socket_error, subscription_callback=feed_data, run_in_background=True, market_depth=False)

# while loop for continue data feed for each symbol
while True:
    try:
        instruments = []

        #Iterate over the rows in the sheet1 - upto 200 rows
        for row in sht.range('A1:B200').value:
            exchange, symbol = row
            if exchange and symbol:
                instruments.append((exchange, symbol))
   
        subscribe_list = []
        for exchange, symbol in instruments:
            subscribe_list.append(alice.get_instrument_by_symbol(exchange, symbol))
            alice.subscribe(subscribe_list)

        # Wait for the data to be populated
        while len(data) < len(subscribe_list):
            pass

        # Create the DataFrame
        df = pd.DataFrame.from_dict(data, orient="index")
        print(df)
        sht.range('C1').value = df
        
    except Exception as e:
        pass
