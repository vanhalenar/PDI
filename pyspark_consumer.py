from html import parser
import os
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import from_json, col, window, row_number
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, LongType, TimestampType
import argparse
import time

def server_counts(df, test_mode=False):
    """Group data by servers and order by most edits."""

    counts = df.groupBy("server_name").count().orderBy("count", ascending=False)

    if test_mode:
        return counts
    
    def write_counts(batch_df, batch_id):
        if not batch_df.isEmpty():
            batch_df.coalesce(1).write.mode("overwrite").json("output/server_counts")
    
    query = counts.writeStream \
        .outputMode("complete") \
        .foreachBatch(write_counts) \
        .start()

    return query

def capture_all(df):
    """(Debug) Capture all wikimedia edit data."""
    query = df.writeStream \
        .outputMode("append") \
        .format("json") \
        .option("path", "output/wikimedia_full") \
        .option("checkpointLocation", "output/checkpoint_full") \
        .start()
    
    return query

def edits_per_minute(df, test_mode=False):
    """Tumbling window query. Find edits per minute on wikipedia servers, excluding bots."""
    df_with_time = df.withColumn(
        "event_time", 
        (col("timestamp")).cast(TimestampType())
    )
    
    windowed = df_with_time \
        .filter(col("bot") == False) \
        .filter(col("server_name").endswith("wikipedia.org")) \
        .withWatermark("event_time", "2 minutes") \
        .groupBy(window(col("event_time"), "1 minute")) \
        .count() \
        .select(
            col("window.start").alias("minute_start"),
            col("window.end").alias("minute_end"),
            col("count").alias("edits_per_minute")
        )
    
    if test_mode:
        return windowed
    
    def write_batch(batch_df, batch_id):
        if not batch_df.isEmpty():
            batch_df.write.mode("append").json("output/edits_per_minute")
    
    query = windowed.writeStream \
        .outputMode("update") \
        .foreachBatch(write_batch) \
        .trigger(processingTime='30 seconds') \
        .start()
    
    return query

def edits_by_type(df, test_mode=False):
    """Groups changes by type (edit, new, log, etc.)."""
    df_filtered = df.filter(col("bot") == False)
    counts = df_filtered.groupBy("type").count().orderBy(col("count").desc())

    if test_mode:
        return counts

    def write_batch(batch_df, batch_id):
        if not batch_df.isEmpty():
            batch_df.coalesce(1).write.mode("overwrite").json("output/edits_by_type")

    query = counts.writeStream \
        .outputMode("complete") \
        .foreachBatch(write_batch) \
        .start()
    
    return query

def user_counts_per_minute(df, test_mode=False):
    """Counts user activities per minute, excluding bots and null users."""
    df_with_time = df.filter((col("bot") == False) & (col("user").isNotNull())) \
                     .withColumn("event_time", col("timestamp").cast(TimestampType()))
    
    windowed = df_with_time.withWatermark("event_time", "1 minute") \
                            .groupBy(window(col("event_time"), "1 minute"), col("user")) \
                            .count() \
                            .withColumnRenamed("count", "activity_count") \
                            .select(
                                col("window.start").alias("window_start"),
                                col("window.end").alias("window_end"),
                                "user",
                                "activity_count"
                            )
    windowed_sorted = windowed.orderBy(col("activity_count").desc())
    if test_mode:
        return windowed_sorted

    def write_batch(batch_df, batch_id):
        if not batch_df.isEmpty():
            batch_df.coalesce(1).write.mode("append").json("output/user_counts")
    
    query = windowed_sorted.writeStream \
                    .outputMode("update") \
                    .foreachBatch(write_batch) \
                    .start()
    
    return query

def avg_edit_length_change(df, test_mode=False):
    """Calculates average edit length change over sliding windows."""
    df_edits = df.filter((col("type") == "edit") & (col("length").isNotNull()))
    df_edits = df_edits.withColumn("delta_length", col("length.new") - col("length.old"))
    df_with_time = df_edits.withColumn("event_time", col("timestamp").cast(TimestampType()))

    windowed = df_with_time \
        .withWatermark("event_time", "2 minutes") \
        .groupBy(window(col("event_time"), "2 minutes", "30 seconds")) \
        .avg("delta_length") \
        .withColumnRenamed("avg(delta_length)", "avg_delta_length") \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("avg_delta_length")
        )
    
    if test_mode:
        return windowed

    def write_batch(batch_df, batch_id):
        if not batch_df.isEmpty():
            batch_df.coalesce(1).write.mode("append").json("output/avg_edit_length")

    query = windowed.writeStream \
        .outputMode("update") \
        .foreachBatch(write_batch) \
        .start()
    
    return query

def main():
    time.sleep(10)
    parser = argparse.ArgumentParser(
                        prog='pyspark_consumer',
                        description='Apache Spark structured streaming project',)
    parser.add_argument('-s', '--server-counts', action='store_true')
    parser.add_argument('-a', '--capture-all', action='store_true')
    parser.add_argument('-e', '--edits-per-minute', action='store_true')
    parser.add_argument('-t', '--edits-by-type', action='store_true')
    parser.add_argument('-u', '--user-counts', action='store_true')
    parser.add_argument('-d', '--avg-edit-length-change', action='store_true')

    parser.add_argument('--test', action='store_true')

    parser.add_argument('-l', '--limit', type=int)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("SSEKafka").config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")  

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
        StructField("wiki", StringType(), True),
        StructField("length", StructType([
            StructField("old", IntegerType(), True),
            StructField("new", IntegerType(), True)
        ]), True),
    ])

    if args.test:
        # test mode - read from local file
        test_file = os.environ.get("TEST_JSON", "tests/testing_data.json")
        df = spark.read.json(test_file)
    else: 
        kafka_servers = os.environ.get('KAFKA_SERVERS', 'localhost:9092')

        df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_servers) \
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

    if args.server_counts:
        if args.test:
            return server_counts(df, test_mode=True)    
        queries.append(server_counts(parsed_wikimedia))
    if args.capture_all:
        queries.append(capture_all(parsed_wikimedia))
    if args.edits_per_minute:
        if args.test:
            return edits_per_minute(df, test_mode=True)
        queries.append(edits_per_minute(parsed_wikimedia))
    if args.edits_by_type:
        if args.test:
            return edits_by_type(df, test_mode=True)
        queries.append(edits_by_type(parsed_wikimedia))
    if args.user_counts:
        if args.test:
            return user_counts_per_minute(df, test_mode=True)
        queries.append(user_counts_per_minute(parsed_wikimedia))
    if args.avg_edit_length_change:
        if args.test:
            return avg_edit_length_change(df, test_mode=True)
        queries.append(avg_edit_length_change(parsed_wikimedia))
    
    if args.limit:
        time.sleep(60)
        for query in queries:
            query.stop()
    else:
        for query in queries:
            query.awaitTermination()

if __name__ == '__main__':
    main()