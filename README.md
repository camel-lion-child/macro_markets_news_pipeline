# macro_markets_news_pipeline

Unified daily pipeline (DuckDB) aggregating:
- Market data (Yahoo Finance)
- Crypto spot benchmark (Coinbase Exchange API)
- Macro data (FRED)
- News intensity & headlines (GDELT + NewsAPI)

## Run
```bash
source .venv/bin/activate
python -m src.run_daily
