import duckdb
import pandas as pd

def query_duckdb():
    # Connect to DuckDB (replace with the path to your DuckDB database)
    conn = duckdb.connect('C:/Users/AvinashR2/dbt-assesment/dev.duckdb')

    # Query the provider_address_agg table
    query = "SELECT * FROM provider_address_agg"
    df = pd.read_sql(query, conn)

    # Print the results
    print(df)

    # Close the connection
    conn.close()
