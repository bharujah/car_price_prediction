from cassandra.cluster import Cluster
import pandas as pd

# Connect to Cassandra
cluster = Cluster(['127.0.0.1'])  # Replace with your Cassandra node IP
session = cluster.connect('cardata')  # Replace with your keyspace

# Path to the CSV file
csv_file_path = '/home/balendran/stockprice_prediction/dataset/CarPrice_Assignment.csv'

# Load the CSV file into a DataFrame
df = pd.read_csv(csv_file_path)

# Ensure that 'car_ID' column exists and sort the DataFrame by 'car_ID' in ascending order
if 'car_ID' not in df.columns:
    raise ValueError("The 'car_ID' column is missing from the CSV file.")
df_sorted = df.sort_values(by='car_ID')

# Define the insert query
insert_query = """
INSERT INTO car_data (
    car_ID, symboling, CarName, fueltype, aspiration, doornumber, carbody, drivewheel,
    enginelocation, wheelbase, carlength, carwidth, carheight, curbweight, enginetype,
    cylindernumber, enginesize, fuelsystem, boreratio, stroke, compressionratio,
    horsepower, peakrpm, citympg, highwaympg, price
) VALUES (%(car_ID)s, %(symboling)s, %(CarName)s, %(fueltype)s, %(aspiration)s, %(doornumber)s, %(carbody)s, %(drivewheel)s,
          %(enginelocation)s, %(wheelbase)s, %(carlength)s, %(carwidth)s, %(carheight)s, %(curbweight)s, %(enginetype)s,
          %(cylindernumber)s, %(enginesize)s, %(fuelsystem)s, %(boreratio)s, %(stroke)s, %(compressionratio)s,
          %(horsepower)s, %(peakrpm)s, %(citympg)s, %(highwaympg)s, %(price)s)
"""

# Insert the sorted data into Cassandra
for _, row in df_sorted.iterrows():
    try:
        row_dict = row.to_dict()  # Convert row to dictionary
        session.execute(insert_query, row_dict)
        print(f"Inserted car_ID {row['car_ID']}")
    except Exception as e:
        print(f"Error inserting row with car_ID {row['car_ID']}: {e}")

print("Data insertion completed.")
