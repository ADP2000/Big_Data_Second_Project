#!/usr/bin/env python3
import sys
import csv
import time

def read_input(file):
    reader = csv.reader(file, delimiter=";")
    for row in reader:
        yield row

def main():
    start_time = time.time()
    input = read_input(sys.stdin)
    header = next(input, None)  # skip header
    
    for fields in input:
        if len(fields) == 12:
            ticker, open_price, close, low, high, volume, date, year, exchange, name, sector, industry = fields
            
            try:
                open_price = float(open_price)
                close = float(close)
                low = float(low)
                high = float(high)
                volume = int(volume)
                year = int(year)
            except ValueError:
                continue

            if year >= 2000:
                key = f"{ticker};{year}"
                value = f"{close};{date}"
            
                print(f"{key}\t{value}")
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    with open('/home/addi/bigData/secondo_progetto/Big_Data_Second_Project/map_reduce/third_question/time_execution/execution_time.txt', 'a') as f:
        f.write(f"Mapper executed in: {elapsed_time:.2f} seconds\n")

if __name__ == "__main__":
    main()
