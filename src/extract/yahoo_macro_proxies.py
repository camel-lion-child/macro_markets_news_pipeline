"""I extract macro market proxy data from Yahoo Finance, standardize the output into a clean long-format dataset, 
and save it as a parquet file for downstream analysis.

Ce script extrait des données proxy de marché macro depuis Yahoo Finance, standardise la sortie dans un format long propre, 
puis la sauvegarde en parquet pour l’analyse ultérieure."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

import pandas as pd
import yfinance as yf


@dataclass(frozen=True)
class YahooMacroConfig:
    #configuration object for macro market data extraction
    tickers: List[str]
    start: str
    end: Optional[str] = None
    out_path: str = "data/raw/yahoo_macro_proxies.parquet"

#map raw yahoo tickers to business friendly metrci names
NAME_MAP = {
    "^TNX": "US_10Y_TREASURY_YIELD",
    "DX-Y.NYB": "US_DOLLAR_INDEX",
    "GC=F": "GOLD",
    "CL=F": "OIL_WTI",
}

#download daily macro proxy data from yahoo finance
def extract_macro(cfg: YahooMacroConfig) -> pd.DataFrame:
    df = yf.download(
        tickers=cfg.tickers,
        start=cfg.start,
        end=cfg.end,
        interval="1d",
        auto_adjust=False,
        group_by="column",
        threads=True,
        progress=False,
    )

    if df is None or df.empty:
        raise RuntimeError("Yahoo returned empty dataframe for macro proxies") #fail fast if no data

    df = df.reset_index() #move date index into a standard column
    
    #flatten multi index columns when multiple tickers are requested
    if isinstance(df.columns, pd.MultiIndex):
        flat = []
        for a, b in df.columns.to_list():
            if b is None or b == "":
                flat.append(str(a))
            else:
                flat.append(f"{a}__{b}")
        df.columns = flat
        
    #keep only closing prices for macro indicator
    value_cols = [c for c in df.columns if c.startswith("Close__")]
    if not value_cols:
        raise RuntimeError(f"No Close prices found. Columns: {list(df.columns)}")

    #convert wide yahoo pouput into long format
    long = df.melt(
        id_vars=["Date"],
        value_vars=value_cols,
        var_name="field_symbol",
        value_name="value",
    )

    long["symbol"] = long["field_symbol"].str.replace("Close__", "", regex=False) #extract ticker symbol from flattened column name
    long["metric"] = long["symbol"].map(NAME_MAP).fillna(long["symbol"]) #map ticker to readable business metric name

    #keep final analytical columns & remove missing values
    out = (
        long[["Date", "symbol", "metric", "value"]]
        .rename(columns={"Date": "date"})
        .dropna(subset=["value"])
    )

    #standardize types & add metadata
    out["date"] = pd.to_datetime(out["date"]).dt.date.astype(str)
    out["source"] = "yahoo_finance"
    out["asset_type"] = "MACRO_MARKET"

    return out.sort_values(["metric", "date"]).reset_index(drop=True) #return clean macro time series sorted by metric & date


def save_parquet(df: pd.DataFrame, out_path: str) -> Path:
    #save extracted macro dataset as parquet
    path = Path(out_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    return path


def run(cfg: YahooMacroConfig) -> Path:
    #run extraction pipeline: extract + save + preview
    df = extract_macro(cfg)
    out = save_parquet(df, cfg.out_path)
    print(f"[Yahoo Macro] Extracted {len(df):,} rows")
    print(f"[Yahoo Macro] Saved to: {out}")
    print(df.tail(5))
    return out


if __name__ == "__main__":
    #example configuration for key macro market proxies
    cfg = YahooMacroConfig(
        tickers=["^TNX", "DX-Y.NYB", "GC=F", "CL=F"],
        start="2024-01-01",
        end=None,
        out_path="data/raw/yahoo_macro_proxies.parquet",
    )
    run(cfg)
