"""I extract daily OHLCV (finance jargon :) open - high - low - close - volume) data from the Coinbase Exchange API, 
standardize it into a clean dataset, and store it as a parquet file for downstream analytics.

Ce script extrait les données OHLCV quotidiennes depuis l’API Coinbase, les standardise dans un format propre, 
puis les sauvegarde en parquet pour l’analyse."""


from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
import requests


@dataclass(frozen=True)
class CoinbaseCandleConfig:
    #configuration object for Coinbase OHLCV extraction
    product_id: str = "BTC-USD"
    granularity: int = 86400  
    start: Optional[str] = None  
    end: Optional[str] = None    
    out_path: str = "data/raw/coinbase_btc_usd_daily.parquet"


def _iso_to_ts(s: str) -> str:
    #convert simple ISO date string to full UTC ISO format required by Coinbase API
    dt = datetime.fromisoformat(s).replace(tzinfo=timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")


def fetch_candles(cfg: CoinbaseCandleConfig) -> pd.DataFrame:
    #build Coinbase API endpoint for OHLCV candles
    url = f"https://api.exchange.coinbase.com/products/{cfg.product_id}/candles"
    params = {"granularity": cfg.granularity}
    #add optional time range filters
    if cfg.start:
        params["start"] = _iso_to_ts(cfg.start)
    if cfg.end:
        params["end"] = _iso_to_ts(cfg.end)

    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()

    if not data:
        raise RuntimeError("Coinbase returned empty candles (check date range or rate limit).") #fail fast if no data

    df = pd.DataFrame(data, columns=["time", "low", "high", "open", "close", "volume"])
    df["date"] = pd.to_datetime(df["time"], unit="s", utc=True).dt.date.astype(str)
    df["symbol"] = "BTC-USD" #add metadata
    df["source"] = "coinbase_exchange"

    out = df[["date", "symbol", "open", "high", "low", "close", "volume", "source"]].copy()
    out = out.sort_values(["symbol", "date"]).reset_index(drop=True)
    return out


def save_parquet(df: pd.DataFrame, out_path: str) -> Path:
    #save OHLCV dataset as parquet file
    path = Path(out_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    return path


def run(cfg: CoinbaseCandleConfig) -> Path:
    #run full extraction pipeline
    df = fetch_candles(cfg)
    out = save_parquet(df, cfg.out_path)
    print(f"[Coinbase] Extracted {len(df):,} rows for {cfg.product_id}.")
    print(f"[Coinbase] Saved to: {out}")
    print(df.tail(5))
    return out


if __name__ == "__main__":
    #example config: fetch BTC daily candles from Coinbase
    cfg = CoinbaseCandleConfig(
        product_id="BTC-USD",
        granularity=86400,
        start="2024-01-01",
        end=None,
        out_path="data/raw/coinbase_btc_usd_daily.parquet",
    )
    run(cfg)
