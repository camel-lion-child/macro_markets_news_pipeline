from __future__ import annotations

import duckdb
import pandas as pd


def init_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS dim_asset (
            symbol TEXT PRIMARY KEY,
            asset_type TEXT,
            source TEXT
        );

        CREATE TABLE IF NOT EXISTS fact_prices_daily (
            date DATE,
            symbol TEXT,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            adj_close DOUBLE,
            volume DOUBLE,
            currency TEXT,
            source TEXT,
            PRIMARY KEY (date, symbol),
            FOREIGN KEY (symbol) REFERENCES dim_asset(symbol)
        );
        """
    )


def upsert_prices(con: duckdb.DuckDBPyConnection, df: pd.DataFrame, asset_type: str) -> None:
    init_schema(con)

    if "adj_close" not in df.columns:
        df = df.copy()
        df["adj_close"] = None

    con.register("stg_prices", df)

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

    con.unregister("stg_prices")


def load_parquet_to_duckdb(parquet_path: str, db_path: str, asset_type: str) -> None:
    df = pd.read_parquet(parquet_path)
    con = duckdb.connect(db_path)
    try:
        upsert_prices(con, df, asset_type=asset_type)
        con.commit()
    finally:
        con.close()


if __name__ == "__main__":
    load_parquet_to_duckdb("data/raw/yahoo_prices.parquet", "warehouse.duckdb", asset_type="ETF")
    print("Loaded Yahoo prices into DuckDB: warehouse.duckdb")
