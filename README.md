# Market & Macro Data Pipeline (Crypto & ETFs)

Overview

This project is a data engineering pipeline that ingests, normalizes, and stores daily market price data related to Bitcoin, Bitcoin ETFs, and macro market proxies.

The main objective is to build a reliable and clean analytical dataset that can be reused for analysis, dashboards, or downstream applications.

The project focuses on data ingestion, data modeling, and data quality validation, not on prediction or advanced modeling.

---

Data Sources

1. Market Prices

Yahoo Finance

Bitcoin ETFs (e.g. IBIT, FBTC, ARKB, BITB)

Coinbase Exchange

BTC-USD daily OHLCV (spot market)

2. Macro Market Proxies (Yahoo Finance)

Daily, market-based macro indicators:

US 10Y Treasury Yield (^TNX)

US Dollar Index (DXY) (DX-Y.NYB)

Gold Futures (GC=F)

WTI Crude Oil (CL=F)

Market-based proxies are preferred over annual macro indicators (CPI, GDP) to ensure daily alignment with crypto prices.

---

macro_markets_news_pipeline/
├── data/
│   └── raw/                     # Raw parquet files
├── eda/
│   └── eda_checks.sql            # SQL-based EDA checks
├── src/
│   ├── extract/                 # Data extraction scripts
│   │   ├── yahoo_prices.py
│   │   ├── coinbase_candles.py
│   │   └── yahoo_macro_proxies.py
│   └── load/                    # DuckDB loaders
│       ├── duckdb_load_prices.py
│       └── duckdb_load_macro_market.py
├── run_eda.py                   # Script to run EDA checks
├── warehouse.duckdb             # Local analytical database
└── README.md

---
Data Model: Tables in DuckDB.

*dim_asset: Metadata for all assets.
symbol
asset_type (ETF, CRYPTO, MACRO_MARKET)
source

*fact_prices_daily: Daily OHLCV prices for crypto and ETFs.
date
symbol
open, high, low, close, adj_close
volume
currency
source

*fact_macro_market_daily: Daily macro market indicators.
date
metric (rates, DXY, gold, oil)
value
source

---

ETL Workflow
*Extract: Fetch data from Yahoo Finance and Coinbase APIs.

*Transform: Normalize schemas and data types. Align all datasets to daily frequency.

*Load: Store data in DuckDB. Use idempotent upserts to avoid duplicates.

---

Exploratory Data Analysis (EDA)

EDA is performed using SQL-based checks, focusing on data quality and consistency rather than visualization.

1. Key checks include:

Row counts per table.

Date coverage per asset.

Missing value detection.

Join validation between prices and macro indicators.

2. EDA Results Summary:

Daily price and macro tables have consistent coverage.

No missing values detected in macro market indicators.

Data volumes match expected date ranges.

Datasets are ready for joins and downstream analysis.

EDA queries are stored in eda/eda_checks.sql and executed via run_eda.py.

---

Design Choices:

DuckDB is used as a lightweight analytical warehouse.

Market-based macro proxies are preferred over annual macro data.

SQL-based EDA is used instead of notebooks for reproducibility.

Project scope is intentionally limited to ensure reliability.

---

Possible Extensions

Risk-on / risk-off market regime views.

News ingestion pipeline (GDELT or NewsAPI).

Dashboards or Streamlit applications.

Feature-ready datasets for machine learning models.
