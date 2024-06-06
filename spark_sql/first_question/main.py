from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, max
from pyspark.sql.functions import lag, col
from pyspark.sql.window import Window
import time
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
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

schema = StructType([
    StructField("ticker", StringType(), True),
    StructField("open_price", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("volume", IntegerType(), True),
    StructField("date", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("exchange", StringType(), True),
    StructField("name", StringType(), True),
    StructField("sector", StringType(), True),
    StructField("industry", StringType(), True)
])

# Leggere il file CSV con lo schema definito
merged_df = spark.read.option("delimiter", ";").csv(input_filepath, header=True, schema=schema)

merged_df = merged_df.orderBy("ticker", "date")

merged_df.show(truncate = False)

# # Registra i DataFrame come tabelle temporanee per l'utilizzo di SparkSQL
merged_df.createOrReplaceTempView("merged_table")

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

result.write \
    .format("csv") \
    .mode("overwrite") \
    .option("header", "true") \
    .save("file:///home/addi/bigData/secondo_progetto/Big_Data_Second_Project/spark_sql/first_question/csv_file")
# Chiudi la sessione Spark
spark.stop()
