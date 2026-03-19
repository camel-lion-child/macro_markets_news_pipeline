"""This script loads macro market indicators from a parquet file into a DuckDB fact table, 
using an upsert strategy to maintain a clean daily time series.

Ce script charge des indicateurs macro-économiques depuis un fichier parquet dans une table de faits DuckDB, 
en utilisant une logique d’upsert pour maintenir une série temporelle quotidienne propre."""

from __future__ import annotations

import duckdb
import pandas as pd


def init_schema(con: duckdb.DuckDBPyConnection) -> None:
    #create fact table to store daily macro & market indicators
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS fact_macro_market_daily (
            date DATE,
            metric TEXT,    #macro indicator name (DXY, rates, gold etc...)
            value DOUBLE,    #metric value
            source TEXT,    #data source (Yahoo...)
            PRIMARY KEY (date, metric)    #ensure 1 value per metric per day
        );
        """
    )


def load_parquet(parquet_path: str, db_path: str = "warehouse.duckdb") -> None:
    df = pd.read_parquet(parquet_path)

    con = duckdb.connect(db_path)
    try:
        init_schema(con) #ensure schema exists before loading data
        con.register("stg_macro_mkt", df) #register dataframe as temporary staging table

        #upsert macro data into fact table (insert or update if already exists)
        con.execute(
            """
            INSERT INTO fact_macro_market_daily (date, metric, value, source)
            SELECT CAST(date AS DATE), metric, value, source
            FROM stg_macro_mkt
            ON CONFLICT(date, metric) DO UPDATE SET
                value = EXCLUDED.value,
                source = EXCLUDED.source;
            """
        )

        con.unregister("stg_macro_mkt") #remove staging table after loading
        con.commit() #commit transaction
    finally:
        con.close() #close database connection


if __name__ == "__main__":
    #load macro market indicators into duckdb warehouse
    load_parquet("data/raw/yahoo_macro_proxies.parquet", "warehouse.duckdb")
    print("Loaded Yahoo macro proxies into DuckDB")
