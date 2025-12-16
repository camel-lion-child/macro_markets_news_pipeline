from __future__ import annotations

import duckdb
import pandas as pd


def init_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS fact_macro_market_daily (
            date DATE,
            metric TEXT,
            value DOUBLE,
            source TEXT,
            PRIMARY KEY (date, metric)
        );
        """
    )


def load_parquet(parquet_path: str, db_path: str = "warehouse.duckdb") -> None:
    df = pd.read_parquet(parquet_path)

    con = duckdb.connect(db_path)
    try:
        init_schema(con)
        con.register("stg_macro_mkt", df)

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

        con.unregister("stg_macro_mkt")
        con.commit()
    finally:
        con.close()


if __name__ == "__main__":
    load_parquet("data/raw/yahoo_macro_proxies.parquet", "warehouse.duckdb")
    print("Loaded Yahoo macro proxies into DuckDB")
