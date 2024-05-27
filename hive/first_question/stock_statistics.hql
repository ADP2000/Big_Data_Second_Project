DROP TABLE stocks;

CREATE TABLE IF NOT EXISTS stocks (
    ticker STRING,
    open_price FLOAT,
    close FLOAT,
    low FLOAT,
    high FLOAT,
    volume INT,
    `date` DATE,
    `year` INT,
    `exchange` STRING,
    `name` STRING,
    sector STRING,
    industry STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

LOAD DATA INPATH '/user/addi/input/big_data_dataset/historical_stock_prices.csv' INTO TABLE stocks;

WITH stock_yearly_stats AS (
    SELECT
        ticker,
        `name`,
        `year`,
        FIRST_VALUE(close) OVER (PARTITION BY ticker, year ORDER BY `date`) AS first_close,
        LAST_VALUE(close) OVER (PARTITION BY ticker, year ORDER BY `date` ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_close,
        MIN(low) OVER (PARTITION BY ticker, `year`) AS min_price,
        MAX(high) OVER (PARTITION BY ticker, `year`) AS max_price,
        AVG(volume) OVER (PARTITION BY ticker, `year`) AS avg_volume
    FROM stocks
)
INSERT OVERWRITE DIRECTORY 'file:///home/addi/bigData/secondo_progetto/Big_Data_Second_Project/hive/first_question/output'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
SELECT
    ticker,
    `name`,
    `year`,
    ROUND(((last_close - first_close) / first_close) * 100, 2) AS percent_change,
    min_price,
    max_price,
    ROUND(avg_volume, 2) AS avg_volume
FROM stock_yearly_stats
GROUP BY ticker, `name`, `year`, first_close, last_close, min_price, max_price, avg_volume
ORDER BY ticker, `year`;
