from cassandra.cluster import Cluster
import pandas as pd
import os

# Connect to Cassandra
cluster = Cluster(['127.0.0.1'])
session = cluster.connect('cardata')

# Path to the output CSV file
output_file_path = '/home/balendran/stockprice_prediction/cardata_input_file.csv'

# Determine the last written record (you might track this via a file, database, etc.)
try:
    existing_df = pd.read_csv(output_file_path)
    last_written_id = existing_df['car_id'].max()  # Replace with your primary key column
    file_exists = True
except FileNotFoundError:
    last_written_id = None
    file_exists = False

# Modify the query to fetch only new records
if last_written_id is not None:
    query = f"SELECT * FROM car_data WHERE car_id > {last_written_id} ALLOW FILTERING;"
else:
    query = "SELECT * FROM car_data;"

rows = session.execute(query)
new_data = pd.DataFrame(list(rows))

# Sort the new data by 'car_id'
if not new_data.empty:
    new_data = new_data.sort_values(by='car_id')  # Replace 'car_ID' with your primary key column
    
    # Write header only if the file doesn't exist (first time creation)
    new_data.to_csv(output_file_path, mode='a', header=not file_exists, index=False)
    
    print(f"New data successfully appended to {output_file_path} in ascending order by car_id.")
else:
    print("No new data to append.")
