from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

import pandas as pd
import yfinance as yf


@dataclass(frozen=True)
class YahooExtractConfig:
    tickers: List[str]
    start: str  
    end: Optional[str] = None
    interval: str = "1d"
    auto_adjust: bool = False  
    out_path: str = "data/raw/yahoo_prices.parquet"


def _flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Flatten yfinance MultiIndex columns to single level strings.
    Example:
      ('Close','FBTC') -> 'Close__FBTC'
      ('Date','')      -> 'Date'
    """
    if not isinstance(df.columns, pd.MultiIndex):
        return df

    flat_cols = []
    for a, b in df.columns.to_list():
        a = str(a) if a is not None else ""
        b = str(b) if b is not None else ""
        if b.strip() == "" or b == "nan":
            flat_cols.append(a)
        else:
            flat_cols.append(f"{a}__{b}")
    out = df.copy()
    out.columns = flat_cols
    return out


def extract_yahoo_prices(cfg: YahooExtractConfig) -> pd.DataFrame:
    if not cfg.tickers:
        raise ValueError("tickers list is empty")

    df = yf.download(
        tickers=cfg.tickers,
        start=cfg.start,
        end=cfg.end,
        interval=cfg.interval,
        auto_adjust=cfg.auto_adjust,  
        group_by="column",
        threads=True,
        progress=False,
    )

    if df is None or df.empty:
        raise RuntimeError("Yahoo returned empty dataframe (check tickers or date range).")

    df = df.reset_index()
    df = _flatten_columns(df)

    if "Date" in df.columns:
        df = df.rename(columns={"Date": "date"})
    elif "date" not in df.columns:
        df = df.rename(columns={df.columns[0]: "date"})

    
    value_cols = [c for c in df.columns if "__" in c]
    if not value_cols:
        raise RuntimeError(f"Could not find price columns after flattening. Columns: {list(df.columns)}")

    long = df.melt(id_vars=["date"], value_vars=value_cols, var_name="field_symbol", value_name="value")

    parts = long["field_symbol"].str.split("__", n=1, expand=True)
    long["field"] = parts[0]
    long["symbol"] = parts[1]
    long = long.drop(columns=["field_symbol"])

    wide = (
        long.pivot_table(index=["date", "symbol"], columns="field", values="value", aggfunc="first")
        .reset_index()
    )

    wide = wide.rename(
        columns={
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Adj Close": "adj_close",
            "Volume": "volume",
        }
    )

    required = {"date", "symbol", "open", "high", "low", "close", "volume"}
    missing = required - set(wide.columns)
    if missing:
        raise RuntimeError(
            f"Missing expected columns: {missing}. "
            f"Got columns: {list(wide.columns)}"
        )

    wide["date"] = pd.to_datetime(wide["date"]).dt.date.astype(str)
    wide["symbol"] = wide["symbol"].astype(str)
    wide["source"] = "yahoo_finance"

    cols = ["date", "symbol", "open", "high", "low", "close"]
    if "adj_close" in wide.columns:
        cols.append("adj_close")
    cols += ["volume", "source"]

    return wide[cols].sort_values(["symbol", "date"]).reset_index(drop=True)


def save_parquet(df: pd.DataFrame, out_path: str) -> Path:
    path = Path(out_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    return path


def run(cfg: YahooExtractConfig) -> Path:
    df = extract_yahoo_prices(cfg)
    out = save_parquet(df, cfg.out_path)
    print(f"[Yahoo] Extracted {len(df):,} rows for {len(cfg.tickers)} tickers.")
    print(f"[Yahoo] Saved to: {out}")
    print(df.tail(5))
    return out


if __name__ == "__main__":
    cfg = YahooExtractConfig(
        tickers=["IBIT", "FBTC", "GBTC", "BITB", "ARKB"],
        start="2024-01-01",
        end=None,
        auto_adjust=False,
        out_path="data/raw/yahoo_prices.parquet",
    )
    run(cfg)



