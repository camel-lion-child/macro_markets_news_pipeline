
import duckdb
con = duckdb.connect("warehouse.duckdb")
print(con.execute("select count(*) from fact_prices_daily").fetchone())
print(con.execute("""
select a.symbol, p.date, p.close, p.volume
from fact_prices_daily p
join dim_asset a on a.asset_id = p.asset_id
order by p.date desc
limit 10
""").df())
con.close()