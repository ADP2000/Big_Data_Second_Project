# import sys
# from collections import defaultdict

# current_ticker = None
# ticker_data = defaultdict(lambda: defaultdict(list))

# # Read from the standard input
# for line in sys.stdin:
#     line = line.strip()
#     ticker, data = line.split('\t')
#     date, year, close, low, high, volume = data.split(',')

#     if current_ticker and current_ticker != ticker:
#         # Process data for the current ticker
#         for year in sorted(ticker_data[current_ticker]):
#             prices = ticker_data[current_ticker][year]
#             first_close = float(prices[0][0])
#             last_close = float(prices[-1][0])
#             percent_change = round(((last_close - first_close) / first_close) * 100, 2)
#             low_price = min(float(p[1]) for p in prices)
#             high_price = max(float(p[2]) for p in prices)
#             avg_volume = sum(float(p[3]) for p in prices) / len(prices)
#             print(f"{current_ticker}\t{year}\t{percent_change}\t{low_price}\t{high_price}\t{avg_volume}")
#         ticker_data = defaultdict(lambda: defaultdict(list))
    
#     current_ticker = ticker
#     ticker_data[ticker][year].append((close_price, low, high, volume))

# # Process the last ticker
# if current_ticker:
#     for year in sorted(ticker_data[current_ticker]):
#         prices = ticker_data[current_ticker][year]
#         first_close = float(prices[0][0])
#         last_close = float(prices[-1][0])
#         percent_change = round(((last_close - first_close) / first_close) * 100, 2)
#         low_price = min(float(p[1]) for p in prices)
#         high_price = max(float(p[2]) for p in prices)
#         avg_volume = sum(float(p[3]) for p in prices) / len(prices)
#         print(f"{current_ticker}\t{year}\t{percent_change}\t{low_price}\t{high_price}\t{avg_volume}")
import sys
import numpy as np
from collections import defaultdict

ticker_data = defaultdict(lambda: {
    "low": [],
    "high": [],
    "close": [],
    "volume": []
})

# Read from the standard input
for line in sys.stdin:
    line = line.strip()
    ticker, data = line.split('\t')
    year, close, low, high, volume = data.split(',')

    # Convert values to float
    close = float(close)
    low = float(low)
    high = float(high)
    volume = float(volume)

    # Use (ticker, year) as the key
    key = (ticker, year)
    ticker_data[key]["low"].append(low)
    ticker_data[key]["high"].append(high)
    ticker_data[key]["close"].append(close)
    ticker_data[key]["volume"].append(volume)

# Process each ticker and year
for (ticker, year), values in ticker_data.items():
    close_prices = values["close"]
    first_close = close_prices[0]
    last_close = close_prices[-1]
    percentual_variation = ((last_close - first_close) / first_close) * 100
    percentual_variation_rounded = round(percentual_variation, 2)

    max_high = max(values["high"])
    min_low = min(values["low"])
    mean_volume = np.mean(values["volume"])

    # Emit the result
    print(f"{ticker}\t{year}\t{percentual_variation_rounded}\t{min_low}\t{max_high}\t{mean_volume}")

