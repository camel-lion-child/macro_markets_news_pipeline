-- This SQL file performs EDA and validation checks on the warehouse by verifying table sizes, data coverage, missing values, 
-- and consistency between crypto, ETF, and macro datasets.

-- Ce fichier SQL réalise des contrôles d’EDA et de validation sur le data warehouse en vérifiant la taille des tables, 
-- la couverture des données, les valeurs manquantes et la cohérence entre les données crypto, ETF et macro.


-- 1. Tables & row counts
SELECT 'dim_asset' AS table_name, COUNT(*) AS rows FROM dim_asset;
SELECT 'fact_prices_daily', COUNT(*) FROM fact_prices_daily;
SELECT 'fact_macro_market_daily', COUNT(*) FROM fact_macro_market_daily;

-- 2. Price data coverage
-- this shows the first available date, last available date & number of rows for each symbol in the price fact table
SELECT
  symbol,
  MIN(date) AS start_date,
  MAX(date) AS end_date,
  COUNT(*)  AS n_rows
FROM fact_prices_daily
GROUP BY symbol
ORDER BY symbol;

-- 3. Macro proxies coverage
-- this verifies date range & row count for each macro indicator
SELECT
  metric,
  MIN(date),
  MAX(date),
  COUNT(*)
FROM fact_macro_market_daily
GROUP BY metric
ORDER BY metric;

-- 4. Missing values checks
SELECT
  symbol,
  SUM(CASE WHEN close IS NULL THEN 1 ELSE 0 END) AS missing_close
FROM fact_prices_daily
GROUP BY symbol;

SELECT
  metric,
  SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END) AS missing_value
FROM fact_macro_market_daily
GROUP BY metric;

-- 5. BTC vs macro join check
-- useful to confirm temporal alignment between crypto & macro dataset
SELECT
  p.date,
  p.close AS btc_close,
  m.metric,
  m.value
FROM fact_prices_daily p
JOIN fact_macro_market_daily m
  ON p.date = m.date
WHERE p.symbol = 'BTC-USD'
ORDER BY p.date DESC
LIMIT 20;

-- 6. ETF vs BTC sanity check
-- useful to compare spot Bitcoin ETFs with the underlying BTC market
SELECT
  p.date,
  p.symbol,
  p.close,
  b.close AS btc_close
FROM fact_prices_daily p
JOIN fact_prices_daily b
  ON p.date = b.date
WHERE p.symbol IN ('IBIT','FBTC')
  AND b.symbol = 'BTC-USD'
ORDER BY p.date DESC
LIMIT 20;
