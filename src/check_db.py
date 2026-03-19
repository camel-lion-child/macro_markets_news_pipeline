"""Quick validation on the price fact table and joins it with the asset dimension to inspect recent market data.

Vérification rapide de la table de faits des prix et la joint avec la dimension des actifs pour inspecter les données de marché récentes."""

import duckdb
con = duckdb.connect("warehouse.duckdb") #connect to duckdb warehouse
print(con.execute("select count(*) from fact_prices_daily").fetchone()) #quick sanity check: count number of rows

#query latest price data joined with asset metadata
print(con.execute("""  
select a.symbol, p.date, p.close, p.volume
from fact_prices_daily p
join dim_asset a on a.asset_id = p.asset_id 
order by p.date desc
limit 10
""").df())
con.close()
