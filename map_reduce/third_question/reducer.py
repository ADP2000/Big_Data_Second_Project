#!/usr/bin/env python3
import sys
import ast
from collections import defaultdict
import time

def calculate_percentage_change(start, end):
    return round(((end - start) / start) * 100, 2)

def main():
    start_time = time.time()
    data = defaultdict(list)

    for line in sys.stdin:
        key, value = line.strip().split('\t')
        ticker, year = key.split(';')
        year = int(year)
        close, date = value.split(';')
        close = float(close)
        
        data[(ticker, year)].append((close, date))

    final_result = []
    ticker_to_year_to_percent_change = defaultdict(dict)

    for (ticker, year), values in data.items():
        values.sort(key=lambda x: x[1])  # Sort by date
        first_close = values[0][0]
        last_close = values[-1][0]
        percent_change = calculate_percentage_change(first_close, last_close)
        ticker_to_year_to_percent_change[ticker][year] = percent_change

    for ticker_a, year_percent_change in ticker_to_year_to_percent_change.items():
        for year in sorted(year_percent_change.keys()):
            if year <= 2016:
                percent_change = year_percent_change[year]
                tickers_same_trend = [ticker_a]
                first_year = (year, percent_change)

                # Check for second and third consecutive years
                second_year, third_year = None, None
                for other_ticker, other_year_percent_change in ticker_to_year_to_percent_change.items():
                    if other_ticker != ticker_a and year + 1 in year_percent_change and year + 2 in year_percent_change:
                        if (year + 1 in other_year_percent_change and
                            other_year_percent_change[year + 1] == year_percent_change[year + 1] and
                            year + 2 in other_year_percent_change and
                            other_year_percent_change[year + 2] == year_percent_change[year + 2]):
                            second_year = (year + 1, other_year_percent_change[year + 1])
                            third_year = (year + 2, other_year_percent_change[year + 2])
                            tickers_same_trend.append(other_ticker)
                
                if second_year and third_year:
                    result = (tickers_same_trend, first_year, second_year, third_year)
                    final_result.append(result)

    for tickers, first_year_result, second_year_result, third_year_result in final_result:
        tickers_string = ", ".join(tickers)
        print(f"{tickers_string}\t{first_year_result[0]}:{first_year_result[1]}\t{second_year_result[0]}:{second_year_result[1]}\t{third_year_result[0]}:{third_year_result[1]}")

    end_time = time.time()
    elapsed_time = end_time - start_time
    with open('/home/addi/bigData/secondo_progetto/Big_Data_Second_Project/map_reduce/third_question/time_execution/execution_time.txt', 'a') as f:
        f.write(f"Reducer executed in: {elapsed_time:.2f} seconds\n")

if __name__ == "__main__":
    main()