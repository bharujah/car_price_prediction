from cassandra.cluster import Cluster

def create_car_data_table(session):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS car_data (
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
        price FLOAT
    );
    """
    session.execute(create_table_query)
    print("Table 'car_data' created successfully.")

def list_tables(session):
    keyspace = 'cardata'  # Replace with your keyspace
    tables_query = f"SELECT table_name FROM system_schema.tables WHERE keyspace_name='{keyspace}';"
    rows = session.execute(tables_query)
    for row in rows:
        print(row.table_name)

# Connect to the Cassandra cluster and keyspace
cluster = Cluster(['127.0.0.1'])
session = cluster.connect('cardata')  # Replace with your keyspace

# Create the table
create_car_data_table(session)

# List tables to confirm creation
print("Tables in the keyspace 'cardata':")
list_tables(session)
