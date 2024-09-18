from pyspark.sql import SparkSession
from pyspark.sql.functions import split
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Create Spark session
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .config("spark.cassandra.connection.host", "127.0.0.1") \
    .getOrCreate()

# Define schema for the data
schema = StructType([
    StructField("fname", StringType(), True),
    StructField("lname", StringType(), True),
    StructField("url", StringType(), True),
    StructField("product", StringType(), True),
    StructField("cnt", IntegerType(), True)
])

# Read from Kafka
kafka_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "userTopic") \
    .load()

# Transform the data
data = kafka_stream \
    .selectExpr("CAST(value AS STRING)") \
    .select(split("value", ",").alias("data")) \
    .selectExpr("data[0] as fname", "data[1] as lname", "data[2] as url", "data[3] as product", "data[4] as cnt")

# Write to Cassandra
query = data.writeStream \
    .outputMode("append") \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="cust_data", keyspace="ecomdata") \
    .option("checkpointLocation", "/tmp/spark/checkpoint") \
    .start()

# Await termination
try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("Streaming job stopped manually")
finally:
    spark.stop()


#running the spark shell
#spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.0,com.datastax.spark:spark-cassandra-connector_2.12:3.1.0 /home/balendran/stockprice_prediction/kafka.py


