# -*- coding: utf-8 -*-
"""
Created on Mon Dec  2 21:26:10 2019

@author: tanvi
"""
import pandas as pd
import math
import os.path
import time
from binance.client import Client
from datetime import timedelta, datetime
from dateutil import parser
from tqdm import tqdm_notebook 
import json
import pymongo
from pymongo import MongoClient


### API

api_key = 'API_KEY'    #Enter your own API-key here
api_secret = 'SECRET_API_KEY' #Enter your own API-secret here

### CONSTANTS
binsizes = {"1m": 1, "5m": 5,"15m":5, "30m": 30, "1h": 60}
batch_size = 750

binance_client = Client(api_key, api_secret)


### FUNCTIONS
def minutes_of_new_data(symbol, kline_size, data, source):
    if len(data) > 0:  old = parser.parse(data["timestamp"].iloc[-1])
    elif source == "binance":
        old = datetime.strptime('1 dec 2019', '%d %b %Y')
    if source == "binance": new = pd.to_datetime(binance_client.get_klines(symbol=symbol, interval=kline_size)[-1][0], unit='ms')
    return old, new

def get_all_binance(symbol, kline_size, save = False):
    data_df=pd.DataFrame()
    oldest_point, newest_point = minutes_of_new_data(symbol, kline_size, data_df, source = "binance")
    delta_min = (newest_point - oldest_point).total_seconds()/60
    available_data = math.ceil(delta_min/binsizes[kline_size])
    if oldest_point == datetime.strptime('1 dec 2019', '%d %b %Y'): print('Downloading all available %s data for %s. Be patient..!' % (kline_size, symbol))
    else: print('Downloading %d minutes of new data available for %s, i.e. %d instances of %s data.' % (delta_min, symbol, available_data, kline_size))
    klines = binance_client.get_historical_klines(symbol, kline_size, oldest_point.strftime("%d %b %Y %H:%M:%S"), newest_point.strftime("%d %b %Y %H:%M:%S"))
    data = pd.DataFrame(klines, columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume','close_time', 'quote_av', 'trades', 'tb_base_av', 'tb_quote_av', 'ignore' ])
    data['timestamp'] = pd.to_datetime(data['timestamp'], unit='ms')
    if len(data_df) > 0:
        temp_df = pd.DataFrame(data)
        data_df = data_df.append(temp_df)
    else: data_df = data
    data_df.set_index('timestamp', inplace=True)
    print('All caught up..!')
    return data_df


data_frame1=pd.DataFrame(columns = [ 'open', 'high', 'low', 'close', 'volume' ])
# For 1 min Binance
binance_symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT","EOSUSDT"]
for symbol in binance_symbols:
    print( "market details",symbol)
    data=get_all_binance(symbol,'1m',save=True)
    data=data.iloc[:,0:5]
    data_frame1=data_frame1.append(data)
    time.sleep(5)
    
    
#for checking quality of data and fill null values by fillna()
s=data_frame1.isnull().sum().sum()
if(s>0):
    print("%d of missing values in data",s)
    data_frame.fillna(method='pad')
else:
    print("data has no null values")

# for 5 min

data_frame2=pd.DataFrame(columns = [ 'open', 'high', 'low', 'close', 'volume' ])

for symbol in binance_symbols:
    print("market details",symbol)
    data=get_all_binance(symbol,'5m',save=True)
    data=data.iloc[:,0:5]
    data_frame2=data_frame2.append(data)
time.sleep(5)

#for 15 min

data_frame3=pd.DataFrame(columns = [ 'open', 'high', 'low', 'close', 'volume' ])
for symbol in binance_symbols:
    print(" market details",symbol)
    data=get_all_binance(symbol,'15m',save=True)
    data=data.iloc[:,0:5]
    data_frame3=data_frame3.append(data)
time.sleep(5)


#for 30 min

data_frame4=pd.DataFrame(columns = [ 'open', 'high', 'low', 'close', 'volume' ])
for symbol in binance_symbols:
    print("%d market details",symbol)
    data=get_all_binance(symbol,'30m',save=True)
    data=data.iloc[:,0:5]
    data_frame4=data_frame4.append(data)
time.sleep(5)

#for 1 hr

data_frame5=pd.DataFrame(columns = [ 'open', 'high', 'low', 'close', 'volume' ])
binance_symbols = ["BTCUSDT", "ETHBTC", "BNBUSDT"]
for symbol in binance_symbols:
    print("%d market details",symbol)
    data=get_all_binance(symbol,'1h',save=True)
    data=data.iloc[:,0:5]
    data_frame5=data_frame5.append(data )
time.sleep(5)

#saving data in mongodb
myclient = pymongo.MongoClient()
mydb = myclient["binance_data"]

customer_collection = mydb["dta_frame"]
#data_frame1.reset_index(inplace=True)
customer_collection.insert_many([data_frame1.to_dict("records"),data_frame2.to_dict("records"),data_frame3.to_dict("records"),data_frame4.to_dict("records"),data_frame5.to_dict("records")])

