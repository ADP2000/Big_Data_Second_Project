from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, max
from pyspark.sql.functions import lag, col
from pyspark.sql.window import Window


import argparse

# create parser and set its arguments
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")

# parse arguments
args = parser.parse_args()
input_filepath = args.input_path


# Inizializza SparkSession
spark = SparkSession.builder \
    .appName("Stock Statistics") \
    .getOrCreate()

merged_df = spark.read.csv(input_filepath, header = True)

merged_df.show()

# Registra i DataFrame come tabelle temporanee per l'utilizzo di SparkSQL
merged_df.createOrReplaceTempView("merged_table")

# Calcola le statistiche richieste per ciascuna azione e anno
stock_statistics_df = spark.sql("""
    SELECT ticker,
           name,
           year,
           ROUND((LAST(close) - FIRST(close)) / FIRST(close) * 100, 2) AS percent_change,
           MIN(low) AS min_price,
           MAX(high) AS max_price,
           AVG(volume) AS avg_volume
    FROM merged_table
    GROUP BY ticker, name, year
    ORDER BY ticker, year
""")

# Mostra il DataFrame con le statistiche
stock_statistics_df.show()

# stock_statistics_df.write \
#     .format("csv") \
#     .mode("overwrite") \
#     .option("header", "true") \
#     .save("file:///home/addi/bigData/secondo_progetto/Big_Data_Second_Project/spark_sql/first_question/csv_file")
# Chiudi la sessione Spark
spark.stop()