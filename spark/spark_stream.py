
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, MapType
from pyspark.sql.functions import from_json, col, unix_timestamp, current_timestamp, date_format, regexp_replace, to_timestamp, lit
from cassandra.cluster import Cluster

def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print("Keyspace created successfully!")

def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.statistic_api_request (
        id TEXT,
        name TEXT,
        type TEXT,
        session_id UUID,
        session_start TIMESTAMP,
        session_end TIMESTAMP,
        event_type TEXT,
        project_id TEXT,
        duration BIGINT,
        tx_value FLOAT,
        month_bucket TEXT,            
        PRIMARY KEY ((month_bucket, project_id), session_end));
    """)

    print("Table statistic_api_request created successfully!")

def create_spark_connection():
    s_conn = None

    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn

def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'ai-event') \
            .option('startingOffsets', 'earliest') \
            .load()
        # use "latest" for streaming processs
        
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

    return spark_df

def create_selection_df_from_kafka(spark_df):
    json_schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("type", StringType(), True),
        StructField("data", StructType([
            StructField("event", StringType(), True),
            StructField("result", StructType([
                StructField("message", StringType(), True)
            ]), True)
        ]), True),
        StructField("extra", StructType([
            StructField("headers", MapType(StringType(), StringType()), True)
        ]), True),
        StructField("session", StructType([
            StructField("id", StringType(), True),
            StructField("start", StringType(), True),
            StructField("end", StringType(), True),
        ]), True)
    ])

    df = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), json_schema).alias('data')).select("data.*")

    # Read JSON data with the specified schema
    final_stream_df = df.select(
        col("id"),
        col("name"),
        col("type"),
        col("session.id").alias("session_id"),
        col("session.start").alias("session_start"),
        col("session.end").alias("session_end"),
        col("data.event").alias("event_type"),
        col("extra.headers.Project-ID").alias("project_id")
    )
    final_stream_df = (final_stream_df
                       .withColumn("duration", ((unix_timestamp("session_end") - unix_timestamp("session_start")).cast("int") * 1000).cast("bigint"))
                       .withColumn("month_bucket", date_format(current_timestamp(), "yyyyMM"))
                       .withColumn("session_id", regexp_replace(col("session_id"), "(.{8})(.{4})(.{4})(.{4})(.{12})", "$1-$2-$3-$4-$5")) 
                       .withColumn("session_start", to_timestamp("session_start"))
                       .withColumn("session_end", to_timestamp("session_end"))
                       .withColumn("tx_value", lit(0.01)))
    # Add month_bucket for query index https://stackoverflow.com/questions/36048660/cassandra-partition-key-for-time-series-data

    print(final_stream_df)
    return final_stream_df

def create_cassandra_connection():
    try:
        # connecting to the cassandra cluster
        cluster = Cluster(['localhost'])

        cas_session = cluster.connect()

        return cas_session
    except Exception as e:
        logging.error(f"Could not create cassandra connection due to {e}")
        return None


def write_to_cassandra_batch(df, epoch_id):
    pass

if __name__ == "__main__":
    # create spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # connect to kafka with spark connection
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)
        session = create_cassandra_connection()

        if session is not None:
            create_keyspace(session)
            create_table(session)

            print("Streaming is being started...")

            streaming_query = (selection_df.writeStream
                               .outputMode("append")
                               .format("org.apache.spark.sql.cassandra")
                               .option("checkpointLocation", "./tmp/checkpoint")
                               .option("keyspace", "spark_streams")
                               .option("table", "statistic_api_request")
                               .start())
            
            streaming_query.awaitTermination()


        # format console to render data into the console using for debug and devlopment
        # query = selection_df.writeStream \
        #     .outputMode("append") \
        #     .format("console") \
        #     .start()
        
        # query.awaitTermination()

        # spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.4.1 spark_stream.py
