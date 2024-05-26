#!/usr/bin/env python3
import sys
import csv
from datetime import datetime

def read_input(file):
    reader = csv.reader(file)
    data = []
    for row in reader:
        data.append(row)
    return data

def main():
    input = read_input(sys.stdin)
    header = input.pop(0)  # skip header
    
    for fields in input:
        if len(fields) == 12:
            ticker, open_price, close, low, high, volume, date, year, exchange, name, sector, industry = fields
            
            # Skip rows where sector or industry is empty
            if not sector or not industry:
                continue
            
            try:
                open_price = float(open_price)
                close = float(close)
                low = float(low)
                high = float(high)
                volume = int(volume)
            except ValueError:
                continue
            
            key = (sector, industry, year)
            value = (ticker, close, volume, date)
            
            print(f"{key}\t{value}")

if __name__ == "__main__":
    main()