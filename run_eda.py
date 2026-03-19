"""This script runs a series of SQL-based EDA checks on a DuckDB warehouse, 
executing each query sequentially and printing the results for validation.

Ce script exécute une série de requêtes SQL d’analyse exploratoire (EDA) sur un entrepôt DuckDB, 
en exécutant chaque requête et en affichant les résultats pour validation."""

import duckdb

DB_PATH = "warehouse.duckdb" #path to duckdb database
EDA_SQL_PATH = "eda/eda_checks.sql" #path to sql file containing EDA queries

con = duckdb.connect(DB_PATH)

with open(EDA_SQL_PATH, "r") as f: #read all sql statements from file
    sql = f.read()

print("Running EDA checks...\n")
for stmt in sql.split(";"): #split SQL file into individual statemants using ";"
    stmt = stmt.strip()
    if not stmt or stmt.startswith("--"): #skip empty statements or comments
        continue
    try:
        result = con.execute(stmt).fetchall() #execute each SQL statement
        print("SQL:")
        print(stmt)
        print("Result:")
        for row in result:
            print(row)
        print("-" * 50)
    except Exception as e:
        print("Error running statement:") #handle errors without stopping the entire scripts
        print(stmt)
        print(e)
        print("-" * 50)

con.close() #close database connection
