import requests
import pandas as pd
import pymongo
from pymongo.errors import ServerSelectionTimeoutError

# API request
url = "https://fakestoreapi.com/products/1"
header = {"Content-Type": "application/json", "Accept-Encoding": "deflate"}

response = requests.get(url, headers=header)
responseData = response.json()

# If 'data' key exists, normalize it, otherwise just convert to a DataFrame
try:
    df = pd.json_normalize(responseData['data'])
except KeyError:
    df = pd.DataFrame([responseData])

# Connect to MongoDB
try:
    client = pymongo.MongoClient("mongodb+srv://harujah:1234@ecom.qryhpgs.mongodb.net/?retryWrites=true&w=majority&appName=eCom", 
                                 serverSelectionTimeoutMS=5000)  # 5 seconds timeout
    db = client["eComdb"]  # Replace with your database name
    collection = db["airflow"]  # Replace with your collection name
    
    # Test the connection
    client.admin.command('ping')
    print("MongoDB connection successful")
    
    # Insert data into MongoDB
    collection.insert_many(df.to_dict("records"))
    print("Data successfully inserted into MongoDB.")
    
except ServerSelectionTimeoutError as err:
    print(f"Failed to connect to MongoDB: {err}")

finally:
    # Close the MongoDB connection
    client.close()
