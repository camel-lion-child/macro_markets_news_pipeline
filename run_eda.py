import duckdb

DB_PATH = "warehouse.duckdb"
EDA_SQL_PATH = "eda/eda_checks.sql"

con = duckdb.connect(DB_PATH)

with open(EDA_SQL_PATH, "r") as f:
    sql = f.read()

print("Running EDA checks...\n")
for stmt in sql.split(";"):
    stmt = stmt.strip()
    if not stmt or stmt.startswith("--"):
        continue
    try:
        result = con.execute(stmt).fetchall()
        print("SQL:")
        print(stmt)
        print("Result:")
        for row in result:
            print(row)
        print("-" * 50)
    except Exception as e:
        print("Error running statement:")
        print(stmt)
        print(e)
        print("-" * 50)

con.close()
