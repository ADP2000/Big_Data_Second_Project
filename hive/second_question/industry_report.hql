DROP TABLE IF EXISTS stocks;
CREATE TABLE IF NOT EXISTS stocks (
    ticker STRING,
    open_price DOUBLE,
    `close` DOUBLE,
    low DOUBLE,
    high DOUBLE,
    volume INT,
    `date` STRING,
    `year` INT,
    `exchange` STRING,
    `name` STRING,
    sector STRING,
    industry STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE;

LOAD DATA INPATH '/user/addi/input/big_data_dataset/historical_stock_prices.csv' INTO TABLE stocks;

WITH stock_changes AS (
    SELECT
        sector,
        industry,
        `year`,
        ticker,
        first_value(`close`) OVER (PARTITION BY sector, industry, `year`, ticker ORDER BY `date`) AS first_close,
        last_value(`close`) OVER (PARTITION BY sector, industry, `year`, ticker ORDER BY `date` ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_close,
        volume
    FROM stocks
    WHERE sector IS NOT NULL AND industry IS NOT NULL
),
industry_metrics AS (
    SELECT
        sector,
        industry,
        `year`,
        SUM(first_close) AS industry_first_total,
        SUM(last_close) AS industry_last_total
    FROM stock_changes
    GROUP BY sector, industry, `year`
),
stock_max_increment AS (
    SELECT
        sector,
        industry,
        `year`,
        ticker,
        (last_close - first_close) / first_close * 100 AS increment_percentage,
        ROW_NUMBER() OVER (PARTITION BY sector, industry, `year` ORDER BY (last_close - first_close) / first_close * 100 DESC) AS rank
    FROM stock_changes
),
stock_max_increment_filtered AS (
    SELECT
        sector,
        industry,
        `year`,
        ticker,
        increment_percentage
    FROM stock_max_increment
    WHERE rank = 1
),
stock_total_volume AS (
    SELECT
        sector,
        industry,
        `year`,
        ticker,
        SUM(volume) AS total_volume
    FROM stock_changes
    GROUP BY sector, industry, `year`, ticker
),
stock_max_total_volume AS (
    SELECT
        sector,
        industry,
        `year`,
        ticker,
        total_volume,
        ROW_NUMBER() OVER (PARTITION BY sector, industry, `year` ORDER BY total_volume DESC) AS volume_rank
    FROM stock_total_volume
),
stock_max_total_volume_filtered AS (
    SELECT
        sector,
        industry,
        `year`,
        ticker,
        total_volume
    FROM stock_max_total_volume
    WHERE volume_rank = 1
)
INSERT OVERWRITE DIRECTORY 'file:///home/addi/bigData/secondo_progetto/Big_Data_Second_Project/hive/second_question/output'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
SELECT
    im.sector,
    im.industry,
    im.year,
    ((im.industry_last_total - im.industry_first_total) / im.industry_first_total) * 100 AS industry_change_percentage,
    smif.ticker AS max_increment_ticker,
    smif.increment_percentage,
    smtvf.ticker AS max_volume_ticker,
    smtvf.total_volume AS max_total_volume
FROM
    industry_metrics im
JOIN
    stock_max_increment_filtered smif ON im.sector = smif.sector AND im.industry = smif.industry AND im.year = smif.year
JOIN
    stock_max_total_volume_filtered smtvf ON im.sector = smtvf.sector AND im.industry = smtvf.industry AND im.year = smtvf.year
ORDER BY
    im.sector, industry_change_percentage DESC;
