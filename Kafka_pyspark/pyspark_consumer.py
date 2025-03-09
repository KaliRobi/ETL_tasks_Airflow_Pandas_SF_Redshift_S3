from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType,  DoubleType, ArrayType


# Define Kafka & Spark session
spark = SparkSession.builder \
    .appName("ViljandiWeatherStream") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/spark_checkpoint") \
    .getOrCreate()

# Define Kafka source
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "Viljandi_weather_stream"

# Define schema (modify based on your API data)
schema = StructType([
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("generationtime_ms", DoubleType(), True),
    StructField("utc_offset_seconds", IntegerType(), True),
    StructField("timezone", StringType(), True),
    StructField("timezone_abbreviation", StringType(), True),
    StructField("elevation", IntegerType(), True),
    
    # Nested hourly_units schema
    StructField("hourly_units", StructType([
        StructField("time", StringType(), True),
        StructField("cloud_cover", StringType(), True),
        StructField("wind_speed_10m", StringType(), True)
    ]), True),
    
    # Nested hourly schema
    StructField("hourly", StructType([
        StructField("time", ArrayType(StringType()), True),
        StructField("cloud_cover", ArrayType(IntegerType()), True),
        StructField("wind_speed_10m", ArrayType(DoubleType()), True)
    ]), True)
])
# Read Kafka stream
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

# Convert Kafka message from binary to JSON
json_df = df.selectExpr("CAST(value AS STRING) as json_data")

# Parse JSON
parsed_df = json_df.select(from_json(col("json_data"), schema).alias("data")).select("data.*")

# consol display
# query = parsed_df.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .start()

# query.awaitTermination()

# sink to db should have its own modude
postgres_url = "jdbc:postgresql://localhost:5432/Viljandi_weather"
postgres_properties = {
    "user": "",
    "password": "",
    "driver": "org.postgresql.Driver"
}

def write_to_postgres(df):
    
    df.write.jdbc(url=postgres_url, table="stg_viljandi_weather", mode="append", properties=postgres_properties)

