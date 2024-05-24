#!/usr/bin/env python3
import sys
import ast
from collections import defaultdict

def calculate_percentage_change(start, end):
    return ((end - start) / start) * 100

def main():
    data = defaultdict(list)
    
    for line in sys.stdin:
        key, value = line.strip().split('\t')
        key = ast.literal_eval(key)
        value = ast.literal_eval(value)
        data[key].append(value)
    
    result = []
    
    for (sector, industry, year), values in data.items():
        values.sort(key=lambda x: (x[0], x[3]))  # Sort by ticker and date

        industry_first_close_sum = {}
        industry_last_close_sum = {}
        
        max_increment = -float('inf')
        max_increment_ticker = None
        max_volume = -float('inf')
        max_volume_ticker = None
        ticker_close_prices = defaultdict(list)
        
        for value in values:
            ticker, close, volume, date = value
            ticker_close_prices[ticker].append((date, close))

            if volume > max_volume:
                max_volume = volume
                max_volume_ticker = (ticker, volume)
        
        for ticker, close_prices in ticker_close_prices.items():
            close_prices.sort(key=lambda x: x[0])  # Sort by date
            
            first_close = close_prices[0][1]
            last_close = close_prices[-1][1]
            
            increment = calculate_percentage_change(first_close, last_close)
            
            if increment > max_increment:
                max_increment = increment
                max_increment_ticker = (ticker, increment)
            
            if ticker not in industry_first_close_sum:
                industry_first_close_sum[ticker] = first_close
            industry_last_close_sum[ticker] = last_close
        
        industry_first_total = sum(industry_first_close_sum.values())
        industry_last_total = sum(industry_last_close_sum.values())
        
        industry_change = calculate_percentage_change(industry_first_total, industry_last_total)
        
        result.append((sector, industry, year, industry_change, max_increment_ticker, max_volume_ticker))
    
    for sector, industry, year, industry_change, max_increment_ticker, max_volume_ticker in sorted(result, key=lambda x: x[3], reverse=True):
        print(f"{sector}\t{industry}\t{year}\t{industry_change:.2f}%\t{max_increment_ticker}\t{max_volume_ticker}")

if __name__ == "__main__":
    main()