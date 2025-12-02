from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, LongType
import argparse
import time

def server_counts(df):
    counts = df.groupBy("server_name").count().orderBy("count", ascending=False)
    
    def write_counts(batch_df, batch_id):
        if not batch_df.isEmpty():
            batch_df.coalesce(1).write.mode("overwrite").json("output/final_server_counts")
    
    query = counts.writeStream \
        .outputMode("complete") \
        .foreachBatch(write_counts) \
        .start()

    return query

def capture_all(df):
    query = df.writeStream \
        .outputMode("append") \
        .format("json") \
        .option("path", "output/wikimedia_full") \
        .option("checkpointLocation", "output/checkpoint_full") \
        .start()
    
    return query

def main():
    parser = argparse.ArgumentParser(
                        prog='pyspark_consumer',
                        description='Apache Spark structured streaming project',)
    parser.add_argument('-s', '--server-counts', action='store_true')
    parser.add_argument('-a', '--capture-all', action='store_true')
    args = parser.parse_args()

    spark = SparkSession.builder.appName("SSEKafka").config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1").getOrCreate()

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

    # extract the 'data' field
    parsed_kafka = df.select(
        from_json(col("value").cast("string"), kafka_schema).alias("kafka_event")
    ).select("kafka_event.data")

    # parse the Wikimedia JSON inside the data field
    parsed_wikimedia = parsed_kafka.select(
        from_json(col("data"), wikimedia_schema).alias("event")
    ).select("event.*")

    queries = []

    if args.count:
        queries.append(server_counts(parsed_wikimedia))
    if args.capture_all:
        queries.append(capture_all(parsed_wikimedia))
        
    time.sleep(30)
    for query in queries:
        query.stop()

if __name__ == '__main__':
    main()