#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, first, last, max as spark_max
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import argparse
import time


# create parser and set its arguments
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")

# parse arguments
args = parser.parse_args()
input_filepath = args.input_path

# start_time = time.time()
# Creare una sessione Spark
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
df = spark.read.option("delimiter", ";").csv(input_filepath, header=True, schema=schema)

# Filtrare le righe dove 'sector' o 'industry' sono nulli
df_filtered = df.filter(df.sector.isNotNull() & df.industry.isNotNull())

# Creare una vista temporanea per usare SQL
df_filtered.createOrReplaceTempView("stocks")

stock_changes_df = spark.sql(
    """
    SELECT
        sector,
        industry,
        year,
        ticker,
        first_value(close) OVER (PARTITION BY sector, industry, year, ticker ORDER BY date) AS first_close,
        last_value(close) OVER (PARTITION BY sector, industry, year, ticker ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_close,
        volume,
    FROM stocks
    WHERE sector IS NOT NULL AND industry IS NOT NULL
    """
)

stock_changes_df.show()

stock_changes_df.createOrReplaceTempView("stock_changes")


industry_metrics_df = spark.sql(
    """
    SELECT
        sector,
        industry,
        year,
        SUM(first_close) AS industry_first_total,
        SUM(last_close) AS industry_last_total
    FROM stock_changes
    GROUP BY sector, industry, year
    """
)

industry_metrics_df.show()
industry_metrics_df.createOrReplaceTempView("industry_metrics")


stock_max_increment_df = spark.sql(
    """
    SELECT
        sector,
        industry,
        year,
        ticker,
        (last_close - first_close) / first_close * 100 AS increment_percentage,
        ROW_NUMBER() OVER (PARTITION BY sector, industry, year ORDER BY (last_close - first_close) / first_close * 100 DESC) AS rank
    FROM stock_changes
    """
)

stock_max_increment_df.createOrReplaceTempView("stock_max_increment")


stock_max_increment_filtered_df = spark.sql(
    """
    SELECT
        sector,
        industry,
        year,
        ticker,
        increment_percentage
    FROM stock_max_increment
    WHERE rank = 1
    """
)
stock_max_increment_filtered_df.createOrReplaceTempView("stock_max_increment_filtered")

stock_total_volume_df = spark.sql(
    """
    SELECT
        sector,
        industry,
        year,
        ticker,
        SUM(volume) AS total_volume
    FROM stock_changes
    GROUP BY sector, industry, year, ticker
    """
)
stock_total_volume_df.createOrReplaceTempView("stock_total_volume")

stock_max_total_volume_df = spark.sql(
    """
    SELECT
        sector,
        industry,
        year,
        ticker,
        total_volume,
        ROW_NUMBER() OVER (PARTITION BY sector, industry, year ORDER BY total_volume DESC) AS volume_rank
    FROM stock_total_volume
    """
)
stock_max_total_volume_df.createOrReplaceTempView("stock_max_total_volume")


stock_max_total_volume_filtered_df = spark.sql(
    """
    SELECT
        sector,
        industry,
        year,
        ticker,
        total_volume
    FROM stock_max_total_volume
    WHERE volume_rank = 1
    """
)
stock_max_total_volume_filtered_df.createOrReplaceTempView("stock_max_total_volume_filtered")

stock_max_total_volume_filtered_df.show()

result = spark.sql(
    """
    SELECT
        im.sector,
        im.industry,
        im.year,
        ((im.industry_last_total - im.industry_first_total) / im.industry_first_total) * 100 AS industry_change_percentage,
        smif.ticker AS max_increment_ticker,
        smif.increment_percentage,
        smv.ticker AS max_volume_ticker,
        smv.total_volume
    FROM
        industry_metrics im
    JOIN
        stock_max_increment_filtered smif ON im.sector = smif.sector AND im.industry = smif.industry AND im.year = smif.year
    JOIN
        stock_max_total_volume_filtered smv ON im.sector = smv.sector AND im.industry = smv.industry AND im.year = smv.year
    ORDER BY
        im.sector, industry_change_percentage DESC

    """
)

# # Mostrare il risultato
result.show()

# Salvare il risultato in un file CSV
# result.write \
#     .format("csv") \
#     .mode("overwrite") \
#     .option("header", "true") \
#     .save("file:///home/addi/bigData/secondo_progetto/Big_Data_Second_Project/spark_sql/second_question/csv_file")
# Fermare la sessione Spark
spark.stop()

# end_time = time.time()
# elapsed_time = end_time - start_time
# with open('/home/addi/bigData/secondo_progetto/Big_Data_Second_Project/spark_sql/second_question/time_execution/execution_time.txt', 'a') as f:
#     f.write(f"{elapsed_time:.2f} seconds\n")