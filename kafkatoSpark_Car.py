from pyspark.sql import SparkSession
from pyspark.sql.functions import split
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# Create Spark session
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .config("spark.cassandra.connection.host", "127.0.0.1") \
    .getOrCreate()

# Define schema for the car data
schema = StructType([
    StructField("car_id", IntegerType(), True),
    StructField("symboling", IntegerType(), True),
    StructField("carname", StringType(), True),
    StructField("fueltype", StringType(), True),
    StructField("aspiration", StringType(), True),
    StructField("doornumber", StringType(), True),
    StructField("carbody", StringType(), True),
    StructField("drivewheel", StringType(), True),
    StructField("enginelocation", StringType(), True),
    StructField("wheelbase", FloatType(), True),
    StructField("carlength", FloatType(), True),
    StructField("carwidth", FloatType(), True),
    StructField("carheight", FloatType(), True),
    StructField("curbweight", FloatType(), True),
    StructField("enginetype", StringType(), True),
    StructField("cylindernumber", StringType(), True),
    StructField("enginesize", FloatType(), True),
    StructField("fuelsystem", StringType(), True),
    StructField("boreratio", FloatType(), True),
    StructField("stroke", FloatType(), True),
    StructField("compressionratio", FloatType(), True),
    StructField("horsepower", FloatType(), True),
    StructField("peakrpm", IntegerType(), True),
    StructField("citympg", FloatType(), True),
    StructField("highwaympg", FloatType(), True),
    StructField("price", FloatType(), True)
    
])

# Read from Kafka
kafka_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "CarTopic") \
    .load()

# Transform the data
data = kafka_stream \
    .selectExpr("CAST(value AS STRING)") \
    .select(split("value", ",").alias("data")) \
    .selectExpr(
        "CAST(data[0] AS INT) as car_id",  # Updated to car_id
        "CAST(data[1] AS INT) as symboling",
        "data[2] as carname",
        "data[3] as fueltype",
        "data[4] as aspiration",
        "data[5] as doornumber",
        "data[6] as carbody",
        "data[7] as drivewheel",
        "data[8] as enginelocation",
        "CAST(data[9] AS FLOAT) as wheelbase",
        "CAST(data[10] AS FLOAT) as carlength",
        "CAST(data[11] AS FLOAT) as carwidth",
        "CAST(data[12] AS FLOAT) as carheight",
        "CAST(data[13] AS FLOAT) as curbweight",
        "data[14] as enginetype",
        "data[15] as cylindernumber",
        "CAST(data[16] AS FLOAT) as enginesize",
        "data[17] as fuelsystem",
        "CAST(data[18] AS FLOAT) as boreratio",
        "CAST(data[19] AS FLOAT) as stroke",
        "CAST(data[20] AS FLOAT) as compressionratio",
        "CAST(data[21] AS FLOAT) as horsepower",
        "CAST(data[22] AS INT) as peakrpm",
        "CAST(data[23] AS FLOAT) as citympg",
        "CAST(data[24] AS FLOAT) as highwaympg",
        "CAST(data[25] AS FLOAT) as price"
        
    )

# Write to Cassandra
query = data.writeStream \
    .outputMode("append") \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="car_data", keyspace="cardata") \
    .option("checkpointLocation", "/tmp/spark/checkpoint") \
    .start()

# Await termination
try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("Streaming job stopped manually")
finally:
    spark.stop()
