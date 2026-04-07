import duckdb

conn = duckdb.connect(r"C:\Users\andra\Documents\GitHub\bts-bdp-exercises\airbnb\airbnb.duckdb")

print(conn.execute("SELECT * FROM main.raw_listings").df())
conn.close()