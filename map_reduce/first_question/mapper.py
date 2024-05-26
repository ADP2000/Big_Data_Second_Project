#!/usr/bin/env python3
import sys
import csv

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
                        
            try:
                open_price = float(open_price)
                close = float(close)
                low = float(low)
                high = float(high)
                volume = int(volume)
            except ValueError:
                continue
            
            key = f"{ticker};{name};{year}"
            value = f"{close};{low};{high};{volume};{date}"
            
            print(f"{key}\t{value}")

if __name__ == "__main__":
    main()

# import sys

# def main():
#     input = sys.stdin
#     header_skipped = False
    
#     for line in input:
#         line = line.strip()
#         # Skip the header line
#         if not header_skipped:
#             header_skipped = True
#             continue
        
#         # Parse the CSV line
#         parts = line.split(',')
#         if len(parts) == 12:
#             ticker, open_price, close, low, high, volume, date, year, exchange, name, sector, industry = parts
#             try:
#                 # Print the values in the desired format
#                 print(f"{ticker}\t{name},{year},{close},{low},{high},{volume}")
#             except ValueError:
#                 continue

# if __name__ == "__main__":
#     main()