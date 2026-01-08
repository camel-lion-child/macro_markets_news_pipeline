
-- 1. Tables & row counts
SELECT 'dim_asset' AS table_name, COUNT(*) AS rows FROM dim_asset;
SELECT 'fact_prices_daily', COUNT(*) FROM fact_prices_daily;
SELECT 'fact_macro_market_daily', COUNT(*) FROM fact_macro_market_daily;

-- 2. Price data coverage
SELECT
  symbol,
  MIN(date) AS start_date,
  MAX(date) AS end_date,
  COUNT(*)  AS n_rows
FROM fact_prices_daily
GROUP BY symbol
ORDER BY symbol;

-- 3. Macro proxies coverage
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
