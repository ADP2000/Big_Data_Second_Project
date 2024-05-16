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

# Calcola la quotazione dell'industria come la somma dei prezzi di chiusura di tutte le azioni dell'industria
industry_prices_df = merged_df.groupBy("sector", "industry", "year") \
    .agg(sum("close").alias("industry_close"))

# # Calcola la variazione percentuale della quotazione dell'industria rispetto all'anno precedente
industry_percent_change_df = industry_prices_df.withColumn("prev_year_close", lag("industry_close", 1).over(Window.partitionBy("sector", "industry").orderBy("year"))) \
    .withColumn("percent_change", ((col("industry_close") - col("prev_year_close")) / col("prev_year_close")) * 100)

industry_percent_change_df.show()

# Calcola la variazione percentuale della quotazione di ciascuna azione
variation_df = merged_df.withColumn("ticker_percent_change", ((merged_df["close"] - merged_df["open"]) / merged_df["open"]) * 100)

# Trova l'azione dell'industria con il maggior incremento percentuale nell'anno
max_percent_change_df = variation_df.groupBy("sector", "industry", "year") \
    .agg(max("ticker_percent_change").alias("max_percent_change")).orderBy("sector", "industry", "year")
    # .withColumnRenamed("ticker", "ticker_max_percent_change").orderBy("sector", "industry")

ticker_max_percent_change_df = variation_df.join(max_percent_change_df, ["sector", "industry", "year"])
ticker_max_percent_change_df.show()

selected_ticker_max_percent_change_df = ticker_max_percent_change_df.select("sector", "industry", "year", "ticker", "max_percent_change").where(col("ticker_percent_change") == col("max_percent_change")).distinct()\
    .withColumnRenamed("ticker", "ticker_max_percent_change")

selected_ticker_max_percent_change_df.show()

# Trova l'azione dell'industria con il maggior volume di transazioni nell'anno
max_volume_df = merged_df.groupBy("sector", "industry", "year").agg(max("volume")\
                .alias("max_volume"))
                # .withColumnRenamed("ticker", "ticker_max_volume")

ticker_max_volume_df = merged_df.join(max_volume_df, ["sector", "industry", "year"])
selected_ticker_max_volume_df = ticker_max_volume_df.select("sector", "industry", "year", "ticker", "max_volume")\
    .where(col("volume") == col("max_volume")).distinct()\
    .withColumnRenamed("ticker", "ticker_max_volume")


selected_ticker_max_volume_df.show()

industry_report_df = industry_percent_change_df.join(selected_ticker_max_percent_change_df, ["sector", "industry", "year"]) \
    .join(selected_ticker_max_volume_df, ["sector", "industry", "year"]) \
    .orderBy("sector", "percent_change", ascending=False)

industry_report_df.show()

# industry_report_selected_df = industry_report_df.select("sector", "industry", "percent_change", "ticker_max_percent_change", "max_percent_change", "ticker_max_volume", "max_volume")

# # Trova il ticker corrispondente al max_percent_change per ciascuna industria e settore
# max_change_ticker_df = industry_report_df.where(col("percent_change") == col("max_percent_change")) \
#     .select("sector", "industry", "ticker_max_percent_change", "max_percent_change")

# # Trova il ticker corrispondente al max_volume per ciascuna industria e settore
# max_volume_ticker_df = industry_report_df.where(col("volume") == col("max_volume")) \
#     .select("sector", "industry", "ticker_max_volume", "max_volume")

# # Unisci i risultati per ottenere il report finale
# final_report_df = industry_report_selected_df.join(max_change_ticker_df, ["sector", "industry"]) \
#     .join(max_volume_ticker_df, ["sector", "industry"])

# # Mostra il report finale
# final_report_df.show(truncate=False)
# Salva il DataFrame finale su Hadoop HDFS
industry_report_df.write \
    .format("csv") \
    .mode("overwrite") \
    .save("file:///home/addi/bigData/secondo_progetto/spark_sql")

# Chiudi la sessione Spark
spark.stop()