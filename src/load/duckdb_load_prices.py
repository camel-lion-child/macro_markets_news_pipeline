"""This script creates a simple warehouse schema in DuckDB and upserts daily market price data from a parquet file 
into dimension and fact tables.

Ce script crée un schéma simple dans DuckDB et insère ou met à jour des données de prix journaliers depuis un fichier parquet 
dans des tables de dimension et de faits."""

from __future__ import annotations

import duckdb
import pandas as pd


def init_schema(con: duckdb.DuckDBPyConnection) -> None:
    #create dimention & fact tables if they don't exist
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS dim_asset (
            symbol TEXT PRIMARY KEY,       #unique asset identifier (BTC, ETH etc...)
            asset_type TEXT,               #asset category (crypto, ETF, stock etc...)
            source TEXT                    #source system of data
        );

        CREATE TABLE IF NOT EXISTS fact_prices_daily (
            date DATE,
            symbol TEXT,
            open DOUBLE,  #open price
            high DOUBLE,    #daily high
            low DOUBLE,    #daily low
            close DOUBLE,    #daily closing price
            adj_close DOUBLE,    #adjusted closing price if available
            volume DOUBLE,    #dalily trend volume
            currency TEXT,    #reporting currency
            source TEXT,    #source system
            PRIMARY KEY (date, symbol),    #1 row per asset per day
            FOREIGN KEY (symbol) REFERENCES dim_asset(symbol)
        );
        """
    )


def upsert_prices(con: duckdb.DuckDBPyConnection, df: pd.DataFrame, asset_type: str) -> None:
    init_schema(con) #ensure warehouse schema exists before loading data
    
    #add missing adj_close column if it doesn't exist in the input dataset
    if "adj_close" not in df.columns:
        df = df.copy()
        df["adj_close"] = None

    con.register("stg_prices", df) #register pandas dataframe as a temporary duckdb table

    #upsert asset metadata into dimention table
    con.execute(
        """
        INSERT INTO dim_asset (symbol, asset_type, source)
        SELECT DISTINCT symbol, ? AS asset_type, source
        FROM stg_prices
        ON CONFLICT(symbol) DO UPDATE SET
            asset_type = EXCLUDED.asset_type,
            source = EXCLUDED.source;
        """,
        [asset_type],
    )

    #upsert daily price observation into fact table
    con.execute(
        """
        INSERT INTO fact_prices_daily (
            date, symbol, open, high, low, close, adj_close, volume, currency, source
        )
        SELECT
            CAST(date AS DATE) AS date,
            symbol,
            open, high, low, close,
            adj_close,
            volume,
            'USD' AS currency,
            source
        FROM stg_prices
        ON CONFLICT(date, symbol) DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            adj_close = EXCLUDED.adj_close,
            volume = EXCLUDED.volume,
            currency = EXCLUDED.currency,
            source = EXCLUDED.source;
        """
    )

    con.unregister("stg_prices") #remove temporary staging table after load


def load_parquet_to_duckdb(parquet_path: str, db_path: str, asset_type: str) -> None:
    df = pd.read_parquet(parquet_path)
    con = duckdb.connect(db_path)
    try:
        upsert_prices(con, df, asset_type=asset_type)
        con.commit()
    finally:
        con.close() #always close database connection


if __name__ == "__main__":
    #load yahoo market data into duckdb warehouse
    load_parquet_to_duckdb("data/raw/yahoo_prices.parquet", "warehouse.duckdb", asset_type="ETF")
    print("Loaded Yahoo prices into DuckDB: warehouse.duckdb")
