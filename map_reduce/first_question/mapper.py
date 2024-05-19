#!/usr/bin/env python

import sys
import csv

# Read from the standard input
for line in sys.stdin:
    line = line.strip()
    # Skip header
    if line.startswith("ticker"):
        continue
    # Parse the CSV line
    parts = line.split(',')
    if len(parts) == 12:
        ticker, open, close, low, high, volume, date, year, exchange, name,	sector,	industry = parts
        try:
            print(f"{ticker}\t{date},{year},{close},{low},{high},{volume}")
        except ValueError:
            continue