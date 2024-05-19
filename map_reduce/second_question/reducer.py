import sys
from collections import defaultdict
import numpy as np

current_key = None
current_data = defaultdict(lambda: {"tickers_percent_change": {}, "closes": [], "tickers_volume": {}})

for line in sys.stdin:
    line = line.strip()
    key, values = line.split('\t')
    sector, industry, year = key.split(',')
    key = (sector, industry, year)
    ticker, close, low, high, volume, percent_change = values.split(',')

    try:
        close = float(close)
        low = float(low)
        high = float(high)
        volume = int(volume)
        percent_change = float(percent_change)
        
        if ticker not in current_data[key]["tickers_percent_change"].keys():
            current_data[key]["tickers_percent_change"][ticker] = percent_change

        if ticker not in current_data[key]["tickers_volume"].keys():
            current_data[key]["tickers_volume"][ticker] = volume

        current_data[key]["closes"].append(close)
    except ValueError:
        continue

for key in current_data.keys():
        # Find the ticker with the maximum percent change


        max_ticker_increment = max(current_data[key]["tickers_percent_change"].items(), key=lambda x: x[1])
        max_percent_change = max_ticker_increment[1]
        ticker_max_percent_change = max_ticker_increment[0]
        
        # Find the ticker with the maximum volume
        max_ticker_volume = max(current_data[key]["tickers_volume"].items(), key=lambda x: x[1])
        max_volume = max_ticker_volume[1]
        ticker_max_volume = max_ticker_volume[0]

        print(f"{key[0]}\t{key[1]}\t{key[2]}\t{ticker_max_percent_change}\t{max_percent_change}\t{ticker_max_volume}\t{max_volume}")
