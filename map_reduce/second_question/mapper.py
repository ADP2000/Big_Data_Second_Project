#!/usr/bin/env python
import sys
import csv
from datetime import datetime

# Read from standard input
for line in sys.stdin:
    line = line.strip()
    if line.startswith("ticker,open,close,low,high,volume,date,year,exchange,name,sector,industry"):
        continue  # Skip the header

    # Split the line into columns
    parts = line.split(',')
    if len(parts) != 12:
        continue  # Skip invalid lines

    ticker, open_price, close_price, low, high, volume, date, year, exchange, name, sector, industry = parts

    try:
        open_price = float(open_price)
        close_price = float(close_price)
        low = float(low)
        high = float(high)
        volume = int(volume)
        
        percent_change = ((close_price - open_price) / open_price) * 100
        
        # Emit key-value pair
        print(f"{sector},{industry},{year}\t{ticker},{close_price},{low},{high},{volume},{percent_change}")
    except ValueError:
        continue
