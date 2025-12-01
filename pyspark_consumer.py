from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, LongType

spark = SparkSession.builder.appName("SSEKafka").config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1").getOrCreate()

print("spark ver:",spark.version)

kafka_schema = StructType([
    StructField("data", StringType(), True),
    StructField("event_type", StringType(), True)
])

wikimedia_schema = StructType([
    StructField("$schema", StringType(), True),
    StructField("meta", StructType([
        StructField("uri", StringType(), True),
        StructField("request_id", StringType(), True),
        StructField("id", StringType(), True),
        StructField("domain", StringType(), True),
        StructField("stream", StringType(), True),
        StructField("dt", StringType(), True),
        StructField("topic", StringType(), True),
        StructField("partition", IntegerType(), True),
        StructField("offset", LongType(), True)
    ]), True),
    StructField("id", LongType(), True),
    StructField("type", StringType(), True),
    StructField("namespace", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("title_url", StringType(), True),
    StructField("comment", StringType(), True),
    StructField("timestamp", LongType(), True),
    StructField("user", StringType(), True),
    StructField("bot", BooleanType(), True),
    StructField("minor", BooleanType(), True),
    StructField("patrolled", BooleanType(), True),
    StructField("server_name", StringType(), True),
    StructField("wiki", StringType(), True)
])

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sse-topic") \
    .option("startingOffsets", "earliest") \
    .load()

# First parse: extract the 'data' field from Kafka message
parsed_kafka = df.select(
    from_json(col("value").cast("string"), kafka_schema).alias("kafka_event")
).select("kafka_event.data")

# Second parse: parse the Wikimedia JSON inside the data field
parsed_wikimedia = parsed_kafka.select(
    from_json(col("data"), wikimedia_schema).alias("event")
).select("event.*")

counts = parsed_wikimedia.groupBy("server_name").count()

query = counts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "False") \
    .start()

query.awaitTermination()