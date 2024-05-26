#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, first, last, max as spark_max
import argparse


# create parser and set its arguments
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")

# parse arguments
args = parser.parse_args()
input_filepath = args.input_path
# Creare una sessione Spark
spark = SparkSession.builder \
    .appName("Stock Statistics") \
    .getOrCreate()

df = spark.read.csv(input_filepath, header = True)


# Filtrare le righe dove 'sector' o 'industry' sono nulli
df_filtered = df.filter(df.sector.isNotNull() & df.industry.isNotNull())

# Selezionare le colonne rilevanti
df_filtered = df_filtered.select("ticker", "close", "volume", "date", "year", "sector", "industry")

# Creare una vista temporanea per usare SQL
df_filtered.createOrReplaceTempView("stocks")

stock_changes_df = spark.sql(
    """
    SELECT
        sector,
        industry,
        year,
        ticker,
        FIRST(close) OVER (PARTITION BY sector, industry, year, ticker ORDER BY date) AS first_close,
        LAST(close) OVER (PARTITION BY sector, industry, year, ticker ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_close,
        volume
    FROM stocks
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


stock_max_increment_df = spark.sql(
    """
    SELECT
        sector,
        industry,
        year,
        ticker,
        MAX((last_close - first_close) / first_close * 100) AS increment_percentage
    FROM stock_changes
    GROUP BY sector, industry, year, ticker
    DISTRIBUTE BY sector, industry, year
    SORT BY sector, industry, year, increment_percentage DESC
    """
)

stock_max_increment_without_ticker_df = spark.sql(
    """
    SELECT
        sector,
        industry,
        year,
        MAX((last_close - first_close) / first_close * 100) AS increment_percentage
    FROM stock_changes
    GROUP BY sector, industry, year
    SORT BY sector, industry, year, increment_percentage DESC
    """
)

stock_max_increment_final_df = stock_max_increment_df.join(stock_max_increment_without_ticker_df, ["sector", "industry", "year", "increment_percentage"]) 

stock_max_increment_final_df.show()

stock_max_volume_df = spark.sql(
    """
    SELECT
        sector,
        industry,
        year,
        ticker,
        MAX(volume) AS max_volume
    FROM stock_changes
    GROUP BY sector, industry, year, ticker
    """
)

stock_max_volume_without_ticker_df = spark.sql(
    """
    SELECT
        sector,
        industry,
        year,
        MAX(volume) AS max_volume
    FROM stock_changes
    GROUP BY sector, industry, year
    """
)

stock_max_volume_final_df = stock_max_volume_df.join(stock_max_volume_without_ticker_df, ["sector", "industry", "year", "max_volume"]) 

stock_max_volume_final_df.show()

stock_max_volume_final_df.createOrReplaceTempView("stock_max_volume")
stock_max_increment_final_df.createOrReplaceTempView("stock_max_increment")
industry_metrics_df.createOrReplaceTempView("industry_metrics")

result = spark.sql(
    """
    SELECT
        im.sector,
        im.industry,
        im.year,
        ((im.industry_last_total - im.industry_first_total) / im.industry_first_total * 100) AS industry_change_percentage,
        smi.ticker AS max_increment_ticker,
        smi.increment_percentage,
        smv.ticker AS max_volume_ticker,
        smv.max_volume
    FROM
        industry_metrics im
    JOIN
        stock_max_increment smi ON im.sector = smi.sector AND im.industry = smi.industry AND im.year = smi.year
    JOIN
        stock_max_volume smv ON im.sector = smv.sector AND im.industry = smv.industry AND im.year = smv.year
    ORDER BY
        im.sector, industry_change_percentage DESC
    """
)


# Query SQL per calcolare la variazione percentuale per ogni industria
# query = """
# WITH stock_changes AS (
#     SELECT
#         sector,
#         industry,
#         year,
#         ticker,
#         FIRST(close) OVER (PARTITION BY sector, industry, year, ticker ORDER BY date) AS first_close,
#         LAST(close) OVER (PARTITION BY sector, industry, year, ticker ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_close,
#         volume,
#         ROW_NUMBER() OVER (PARTITION BY sector, industry, year ORDER BY volume DESC) AS volume_rank
#     FROM stocks
# ),
# industry_metrics AS (
#     SELECT
#         sector,
#         industry,
#         year,
#         SUM(first_close) AS industry_first_total,
#         SUM(last_close) AS industry_last_total
#     FROM stock_changes
#     GROUP BY sector, industry, year
# ),
# stock_max_increment AS (
#     SELECT
#         sector,
#         industry,
#         year,
#         ticker,
#         MAX((last_close - first_close) / first_close * 100) AS increment_percentage
#     FROM stock_changes
#     GROUP BY sector, industry, year, ticker
#     DISTRIBUTE BY sector, industry, year
#     SORT BY sector, industry, year, increment_percentage DESC
# ),
# stock_max_volume AS (
#     SELECT
#         sector,
#         industry,
#         year,
#         ticker,
#         volume AS max_volume
#     FROM stock_changes
#     WHERE volume_rank = 1
# )

# SELECT
#     im.sector,
#     im.industry,
#     im.year,
#     ((im.industry_last_total - im.industry_first_total) / im.industry_first_total * 100) AS industry_change_percentage,
#     smi.ticker AS max_increment_ticker,
#     smi.increment_percentage,
#     smv.ticker AS max_volume_ticker,
#     smv.max_volume
# FROM
#     industry_metrics im
# JOIN
#     stock_max_increment smi ON im.sector = smi.sector AND im.industry = smi.industry AND im.year = smi.year
# JOIN
#     stock_max_volume smv ON im.sector = smv.sector AND im.industry = smv.industry AND im.year = smv.year
# ORDER BY
#     im.sector, industry_change_percentage DESC
# """

# # Eseguire la query
# result = spark.sql(query)

# # Mostrare il risultato
result.show()

# # Salvare il risultato in un file CSV
# result.write \
    # .format("csv") \
    # .mode("overwrite") \
    # .option("header", "true") \
    # .save("file:///home/addi/bigData/secondo_progetto/Big_Data_Second_Project/spark_sql/second_question/csv_file2")
# Fermare la sessione Spark
spark.stop()