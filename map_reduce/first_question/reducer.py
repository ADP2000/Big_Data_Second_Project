#!/usr/bin/env python3
import sys
from collections import defaultdict
import time


def calculate_percentage_change(start, end):
    return ((end - start) / start) * 100

def parse_key_value(line):
    key, value = line.strip().split('\t')
    key_parts = key.split(';')
    ticker = key_parts[0]
    name = key_parts[1]
    year = key_parts[2]
    
    value_parts = value.split(';')
    close = float(value_parts[0])
    low = float(value_parts[1])
    high = float(value_parts[2])
    volume = int(value_parts[3])
    date = value_parts[4]
    
    return (ticker, name, year), (close, low, high, volume, date)

def main():
    start_time = time.time()

    data = defaultdict(list)
    
    for line in sys.stdin:
        key, value = parse_key_value(line)
        data[key].append(value)
    
    result = []
    
    for (ticker, name, year), values in data.items():
        values.sort(key=lambda x: x[4])  # Sort by date
        
        first_close = values[0][0]
        last_close = values[-1][0]
        min_price = min(v[1] for v in values)
        max_price = max(v[2] for v in values)
        avg_volume = sum(v[3] for v in values) / len(values)
        
        percentage_change = round(calculate_percentage_change(first_close, last_close), 2)
        
        result.append((ticker, name, year, percentage_change, min_price, max_price, avg_volume))
    
    for ticker, name, year, percentage_change, min_price, max_price, avg_volume in sorted(result, key=lambda x: (x[0], x[2])):
        print(f"{ticker}\t{name}\t{year}\t{percentage_change:.2f}\t{min_price}\t{max_price}\t{avg_volume:.2f}")
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    with open('/home/addi/bigData/secondo_progetto/Big_Data_Second_Project/map_reduce/first_question/time_execution/execution_time.txt', 'a') as f:
        f.write(f"Reducer executed in: {elapsed_time:.2f} seconds\n")


if __name__ == "__main__":
    main()
