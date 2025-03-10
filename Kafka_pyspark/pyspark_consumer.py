from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, ArrayType



checkpoint_dir = "file:///C:/tmp/spark-checkpoint/"
# Kafka and Spark session
spark = SparkSession.builder \
    .appName("ViljandiWeatherStream") \
    .config("spark.sql.streaming.checkpointLocation", checkpoint_dir) \
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
    .config("spark.hadoop.io.native.lib", "false") \
    .config("spark.hadoop.fs.local.block.size", "134217728") \
    .getOrCreate()

# kafka source
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "Viljandi_weather_stream"

# Sink to db should have its own module
postgres_url = "jdbc:postgresql://localhost:5432/Viljandi_weather"
postgres_properties = {
    "user": "",
    "password": "",
    "driver": "org.postgresql.Driver"
}

def write_to_postgres(df):
    df.write.jdbc(url=postgres_url, table="stg_viljandi_weather", mode="append", properties=postgres_properties)

# schema
schema = StructType([
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("generationtime_ms", DoubleType(), True),
    StructField("utc_offset_seconds", IntegerType(), True),
    StructField("timezone", StringType(), True),
    StructField("timezone_abbreviation", StringType(), True),
    StructField("elevation", IntegerType(), True),
    StructField("hourly_units", StructType([
        StructField("time", StringType(), True),
        StructField("cloud_cover", StringType(), True),
        StructField("wind_speed_10m", StringType(), True)
    ]), True),
    StructField("hourly", StructType([
        StructField("time", ArrayType(StringType()), True),
        StructField("cloud_cover", ArrayType(IntegerType()), True),
        StructField("wind_speed_10m", ArrayType(DoubleType()), True)
    ]), True)
])

# Read Kafka stream
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

# Convert Kafka message from binary to JSON and later transfer the data here
json_df = kafka_stream.selectExpr("CAST(value AS STRING) as json_data")

# parse JSON
parsed_df = json_df.select(from_json(col("json_data"), schema).alias("data")).select("data.*")

flattened_df = parsed_df.select(
    explode(col("hourly.time")).alias("time"),
    explode(col("hourly.cloud_cover")).alias("cloud_cover"),
    explode(col("hourly.wind_speed_10m")).alias("wind_speed_10m"),
    col("hourly_units.cloud_cover").alias("cloud_cover_unit"),
    col("hourly_units.wind_speed_10m").alias("wind_speed_10m_unit")
)

# write to PostgreSQL
write_to_postgres(flattened_df)

# Console display
query = parsed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
