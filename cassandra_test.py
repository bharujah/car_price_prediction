from cassandra.cluster import Cluster
import pandas as pd

# Connect to Cassandra
cluster = Cluster(['127.0.0.1'])  # Replace with your Cassandra node IP
session = cluster.connect('ecomdata')  # Replace with your keyspace

# Query data
query = "SELECT * FROM cust_data;"
rows = session.execute(query)

# Convert to DataFrame
df = pd.DataFrame(rows, columns=rows[0]._asdict().keys())
print(df)
