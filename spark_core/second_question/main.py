from pyspark import SparkConf, SparkContext
import argparse
from collections import defaultdict
from operator import add
import csv
from io import StringIO
import time


def parse_line(line):
    reader = csv.reader(StringIO(line))
    parts = next(reader)
    if len(parts) == 12:
        ticker, open_price, close, low, high, volume, date, year, exchange, name, sector, industry = parts
        if not sector or not industry:
            return None
        
        try:
            close = float(close)
            volume = float(volume)
            year = int(year)
            return (sector, industry, year, ticker, close, volume, date)
        except ValueError:
            return None
    return None


def calculate_industry_stats(data):
    (sector, industry, year), values = data
    values = list(values)
    
    # Initialize data structures
    industry_first_close_sum = defaultdict(float)
    industry_last_close_sum = defaultdict(float)
    
    max_increment = -float('inf')
    max_increment_ticker = None
    max_volume = -float('inf')
    max_volume_ticker = None
    
    ticker_closes = {}
    
    # ticker_volumes = defaultdict(list)
    
    # Collect data for calculations
    for value in values:
        ticker, close, volume, date = value

        if volume > max_volume:
            max_volume = volume
            max_volume_ticker = (ticker, volume)

        if ticker not in ticker_closes:
            ticker_closes[ticker] = [(date, close)]
        ticker_closes[ticker].append((date, close))
    
    # Calculate industry stats
    for ticker, values in ticker_closes.items():
        values_sorted = sorted(values, key=lambda x: x[0])
        first_close = values_sorted[0][1]
        last_close = values_sorted[-1][1]
        increment = (last_close - first_close) / first_close * 100

        industry_first_close_sum[year] += first_close
        industry_last_close_sum[year] += last_close
        
        if increment > max_increment:
            max_increment = increment
            max_increment_ticker = (ticker, increment)
    
    industry_first_total = industry_first_close_sum[year]
    industry_last_total = industry_last_close_sum[year]
    industry_change = (industry_last_total - industry_first_total) / industry_first_total * 100
    
    return (sector, industry, year, industry_change, max_increment_ticker, max_volume_ticker)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_path", type=str, help="Input file path")
    parser.add_argument("--output_path", type=str, help="Output folder path")

    # Parse arguments
    args = parser.parse_args()
    input_filepath = args.input_path
    output_filepath = args.output_path

    start_time = time.time()

    
    conf = SparkConf().setAppName("Industry Statistics")
    sc = SparkContext(conf=conf)

    lines = sc.textFile(input_filepath)
    header = lines.first()
    data = lines.filter(lambda line: line != header)
    
    parsed_data = data.map(parse_line).filter(lambda x: x is not None)
    
    grouped_data = parsed_data.map(lambda x: ((x[0], x[1], x[2]), (x[3], x[4], x[5], x[6])))\
                              .groupByKey()
    
    stats = grouped_data.map(calculate_industry_stats)
    
    sorted_stats = stats.sortBy(lambda x: (x[0], -x[3]))  # Sort by sector and descending industry_change

    formatted_results = sorted_stats.map(lambda x: f"{x[0]}\t{x[1]}\t{x[2]}\t{x[3]:.2f}\t{x[4][0]}\t{x[4][1]:.2f}\t{x[5][0]}\t{x[5][1]:.2f}")

    # Save the results to the output path
    formatted_results.saveAsTextFile(output_filepath)
    sc.stop()

    end_time = time.time()
    elapsed_time = end_time - start_time
    with open('/home/addi/bigData/secondo_progetto/Big_Data_Second_Project/spark_core/second_question/time_execution/execution_time.txt', 'a') as f:
        f.write(f"{elapsed_time:.2f} seconds\n")