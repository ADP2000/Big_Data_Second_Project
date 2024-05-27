from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, max
from pyspark.sql.functions import lag, col
from pyspark.sql.window import Window
import time

import argparse



# create parser and set its arguments
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")

# parse arguments
args = parser.parse_args()
input_filepath = args.input_path

start_time = time.time()
# Inizializza SparkSession
spark = SparkSession.builder \
    .appName("Stock Statistics") \
    .getOrCreate()


merged_df = spark.read.csv(input_filepath, header = True)
merged_df = merged_df.orderBy("ticker", "date")

merged_df.show(truncate = False)

# # Registra i DataFrame come tabelle temporanee per l'utilizzo di SparkSQL
merged_df.createOrReplaceTempView("merged_table")

# Calcola le statistiche richieste per ciascuna azione e anno
# stock_statistics_df = spark.sql("""
#     SELECT ticker,
#            name,
#            year,
#            ROUND((LAST(close) - FIRST(close)) / FIRST(close) * 100, 2) AS percent_change,
#            MIN(low) AS min_price,
#            MAX(high) AS max_price,
#            AVG(volume) AS avg_volume
#     FROM merged_table
#     GROUP BY ticker, name, year
#     ORDER BY ticker, year
# """)

stock_statistics_df = spark.sql("""
    SELECT
        ticker,
        name,
        year,
        FIRST_VALUE(close) OVER (PARTITION BY ticker, year ORDER BY date) AS first_close,
        LAST_VALUE(close) OVER (PARTITION BY ticker, year ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_close,
        MIN(low) OVER (PARTITION BY ticker, year) AS min_price,
        MAX(high) OVER (PARTITION BY ticker, year) AS max_price,
        AVG(volume) OVER (PARTITION BY ticker, year) AS avg_volume
    FROM merged_table
""")

# Mostra il DataFrame con le statistiche
stock_statistics_df.show()
stock_statistics_df.createOrReplaceTempView("stock_yearly_stats")

result = spark.sql(
    """
    SELECT
        ticker,
        name,
        year,
        ROUND(((last_close - first_close) / first_close) * 100, 2) AS percent_change,
        min_price,
        max_price,
        ROUND(avg_volume, 2) AS avg_volume
    FROM stock_yearly_stats
    GROUP BY ticker, name, year, first_close, last_close, min_price, max_price, avg_volume
    ORDER BY ticker, year

    """
)
result.show()

# result.write \
#     .format("csv") \
#     .mode("overwrite") \
#     .option("header", "true") \
#     .save("file:///home/addi/bigData/secondo_progetto/Big_Data_Second_Project/spark_sql/first_question/csv_file")
# Chiudi la sessione Spark
spark.stop()

# end_time = time.time()
# elapsed_time = end_time - start_time
# with open('/home/addi/bigData/secondo_progetto/Big_Data_Second_Project/spark_sql/first_question/time_execution/execution_time.txt', 'a') as f:
#     f.write(f"{elapsed_time:.2f} seconds\n")