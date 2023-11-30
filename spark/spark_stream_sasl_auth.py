# Set your Kafka broker and topic
kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "ai-event"

# Set your Kafka security configurations
kafka_security_protocol = "SASL_PLAINTEXT"
kafka_sasl_mechanism = "SCRAM-SHA-512"

from pyspark.sql import SparkSession

spark = SparkSession.builder\
    .master("local")\
    .appName("kafka-example")\
    .config("spark.kafka.sasl.jaas.config", "file:./custom_jaas.conf") \
    .getOrCreate()

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("kafka.security.protocol", kafka_security_protocol) \
    .option("kafka.sasl.mechanism", kafka_sasl_mechanism) \
    .option("startingoffsets", "latest") \
    .option("subscribe", "ai-event") \
    .option("group.id", "hand-on") \
    .load()

query = df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()

# spark-submit --conf "spark.driver.extraJavaOptions=-Djava.security.auth.login.config=./custom_jaas.conf" script.py
