from pyspark import SparkContext, SparkConf
import argparse
import time

# Funzione per calcolare la variazione percentuale tra due valori
def calculate_percentage_change(start, end):
    return round(((end - start) / start) * 100, 2)

# Funzione per trovare i trend comuni per almeno 3 anni consecutivi
def find_common_trends(ticker_to_year_to_percent_change):
    final_result = []
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
    return final_result

# Funzione per leggere le righe dal file CSV e restituire un tuple con ticker, anno e variazione percentuale
def parse_line(line):
    fields = line.split(';')
    ticker = fields[0]
    year = int(fields[7])
    close = float(fields[2])
    date = fields[6]  # Assuming the date is in the 4th field
    return (ticker, year, close, date)

# Funzione per calcolare la variazione percentuale annuale
def calculate_annual_percentage_change(values):
    sorted_values = sorted(values, key=lambda x: x[3])  # Sort by date
    first_close = sorted_values[0][2]
    last_close = sorted_values[-1][2]
    percentage_change = calculate_percentage_change(first_close, last_close)
    return (sorted_values[0][0], sorted_values[0][1], percentage_change)

# Funzione per formattare il risultato come richiesto
def format_result(record):
    tickers, first_year, second_year, third_year = record
    trend_str = f"{first_year[0]}:{first_year[1]}%, {second_year[0]}:{second_year[1]}%, {third_year[0]}:{third_year[1]}%"
    return f"{', '.join(tickers)}\t{trend_str}"

# Crea lo SparkContext
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input file path")
parser.add_argument("--output_path", type=str, help="Output folder path")

# Parse arguments
args = parser.parse_args()
input_filepath = args.input_path
output_filepath = args.output_path
start_time = time.time()

conf = SparkConf().setAppName("Stocks Trends")
sc = SparkContext(conf=conf)

lines = sc.textFile(input_filepath)
header = lines.first()

data = lines.filter(lambda line: line != header)

# Estrai ticker, anno, prezzo di chiusura e data per ogni riga
parsed_data = data.map(parse_line)

# Filtra i dati per iniziare dall'anno 2000
filtered_data = parsed_data.filter(lambda x: x[1] >= 2000)

# Raggruppa i dati per ticker e anno e calcola la variazione percentuale annuale
grouped_data = filtered_data.groupBy(lambda x: (x[0], x[1])).mapValues(list)
annual_percentage_change = grouped_data.map(lambda x: calculate_annual_percentage_change(x[1]))

# Convertire in una struttura dati adatta per trovare trend comuni
ticker_to_year_to_percent_change = annual_percentage_change.map(lambda x: (x[0], (x[1], x[2]))).groupByKey().mapValues(dict).collectAsMap()

# Trova i trend comuni per almeno 3 anni consecutivi
common_trends_rdd = sc.parallelize(find_common_trends(ticker_to_year_to_percent_change))

# Formatta i risultati
formatted_result = common_trends_rdd.map(format_result)

# Salva i risultati in un file di output
formatted_result.saveAsTextFile(output_filepath)

# Ferma lo SparkContext
sc.stop()

end_time = time.time()
elapsed_time = end_time - start_time
with open('/home/addi/bigData/secondo_progetto/Big_Data_Second_Project/spark_core/third_question/time_execution/execution_time.txt', 'a') as f:
    f.write(f"{elapsed_time:.2f} seconds\n")