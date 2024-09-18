import pandas as pd
from cassandra.cluster import Cluster
import numpy as np

# Load CSV into DataFrame
df = pd.read_csv('/home/balendran/stockprice_prediction/Car_Predictions_Output_File.csv')

# Replace NaN values with None (Cassandra requires None for null)
df = df.replace({np.nan: None})

# Inspect DataFrame columns and sample rows
print("Columns in DataFrame:", df.columns)
print("Sample rows from DataFrame:\n", df.head())

# Ensure 'Price_Predictions' column exists
if 'Price_Predictions' not in df.columns:
    df['Price_Predictions'] = None  # or use a default value

# Connect to Cassandra
cluster = Cluster(['127.0.0.1'])  # Replace with your Cassandra node IP
session = cluster.connect('cardata')  # Replace with your keyspace

# Create a table to store the CSV data
create_table_query = """
CREATE TABLE IF NOT EXISTS car_predictions_output (
    car_ID INT PRIMARY KEY,
    symboling INT,
    CarName TEXT,
    fueltype TEXT,
    aspiration TEXT,
    doornumber TEXT,
    carbody TEXT,
    drivewheel TEXT,
    enginelocation TEXT,
    wheelbase FLOAT,
    carlength FLOAT,
    carwidth FLOAT,
    carheight FLOAT,
    curbweight FLOAT,
    enginetype TEXT,
    cylindernumber TEXT,
    enginesize FLOAT,
    fuelsystem TEXT,
    boreratio FLOAT,
    stroke FLOAT,
    compressionratio FLOAT,
    horsepower FLOAT,
    peakrpm INT,
    citympg FLOAT,
    highwaympg FLOAT,
    price FLOAT,
    Phase TEXT,
    Price_Predictions FLOAT
);
"""
session.execute(create_table_query)

# Define the insert query with the new columns
insert_query = """
INSERT INTO car_predictions_output (
    car_ID, symboling, CarName, fueltype, aspiration, doornumber, carbody, drivewheel,
    enginelocation, wheelbase, carlength, carwidth, carheight, curbweight, enginetype,
    cylindernumber, enginesize, fuelsystem, boreratio, stroke, compressionratio,
    horsepower, peakrpm, citympg, highwaympg, price, Phase, Price_Predictions
) VALUES (%(car_ID)s, %(symboling)s, %(CarName)s, %(fueltype)s, %(aspiration)s, %(doornumber)s, %(carbody)s, %(drivewheel)s,
          %(enginelocation)s, %(wheelbase)s, %(carlength)s, %(carwidth)s, %(carheight)s, %(curbweight)s, %(enginetype)s,
          %(cylindernumber)s, %(enginesize)s, %(fuelsystem)s, %(boreratio)s, %(stroke)s, %(compressionratio)s,
          %(horsepower)s, %(peakrpm)s, %(citympg)s, %(highwaympg)s, %(price)s, %(Phase)s, %(Price_Predictions)s)
"""

# Insert/Update data into Cassandra table
for _, row in df.iterrows():
    try:
        row_dict = row.to_dict()  # Convert row to dictionary
        session.execute(insert_query, row_dict)
    except Exception as e:
        print(f"Error inserting row {row['car_ID']}: {e}")

print("Data insertion completed")
