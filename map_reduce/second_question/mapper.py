#!/usr/bin/env python3
import sys
import csv
from datetime import datetime

def read_input(file):
    reader = csv.reader(file)
    for row in reader:
        yield row

def main():
    input = read_input(sys.stdin)
    header = next(input)  # skip header
    
    for fields in input:
        ticker, open_price, close, low, high, volume, date, year, exchange, name, sector, industry = fields
        try:
            open_price = float(open_price)
            close = float(close)
            low = float(low)
            high = float(high)
            volume = int(volume)
            date = datetime.strptime(date, '%Y-%m-%d')
        except ValueError:
            continue
        
        key = (sector, industry, year)
        value = (ticker, close, volume, date.strftime('%Y-%m-%d'))
        
        print(f"{key}\t{value}")

if __name__ == "__main__":
    main()