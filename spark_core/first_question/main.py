from pyspark import SparkConf, SparkContext
import argparse
import csv
from io import StringIO

def parse_line(line):
    reader = csv.reader(StringIO(line))
    parts = next(reader)
    if len(parts) == 12:
        ticker, open_price, close, low, high, volume, date, year, exchange, name, sector, industry = parts
        
        try:
            close = float(close)
            low = float(low)
            high = float(high)
            volume = float(volume)
            year = int(year)
            return (ticker, name, year, close, low, high, volume)
        except ValueError:
            return None
    return None

def calculate_stats(data):
    (ticker, name, year), values = data
    values = list(values)
    
    first_close = values[0][0]
    last_close = values[-1][0]
    percent_change = round(((last_close - first_close) / first_close) * 100, 2)
    min_price = min(value[1] for value in values)
    max_price = max(value[2] for value in values)
    avg_volume = sum(value[3] for value in values) / len(values)
    
    return (ticker, name, year, percent_change, min_price, max_price, avg_volume)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_path", type=str, help="Input file path")
    parser.add_argument("--output_path", type=str, help="Output folder path")

    # Parse arguments
    args = parser.parse_args()
    input_filepath = args.input_path
    output_filepath = args.output_path
    
    conf = SparkConf().setAppName("Stock Statistics")
    sc = SparkContext(conf=conf)

    lines = sc.textFile(input_filepath)
    header = lines.first()
    data = lines.filter(lambda line: line != header)
    
    parsed_data = data.map(parse_line).filter(lambda x: x is not None)
    
    grouped_data = parsed_data.map(lambda x: ((x[0], x[1], x[2]), (x[3], x[4], x[5], x[6])))\
                              .groupByKey()
    
    stats = grouped_data.map(calculate_stats)

    # Collect the results and sort them
    sorted_stats = stats.sortBy(lambda x: (x[0], x[2]))  # Sort by ticker and year

    # Format the results as tab-separated strings
    formatted_results = sorted_stats.map(lambda x: f"{x[0]}\t{x[1]}\t{x[2]}\t{x[3]}\t{x[4]}\t{x[5]}\t{x[6]}")

    # Save the results to the output path
    formatted_results.saveAsTextFile(output_filepath)
    sc.stop()