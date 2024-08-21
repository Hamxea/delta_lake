from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("KafkaStreamExample") \
    .getOrCreate()

# Set Kafka parameters
kafka_bootstrap_servers = "kafka_broker_1:19092,kafka_broker_2:19093,kafka_broker_3:19094"
kafka_topic_name = "names_topic"


# Construct a streaming DataFrame that reads from test-topic
orders_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic_name) \
    .option("startingOffsets", "latest") \
    .load()

print("Printing Schema of orders_df: ")
orders_df.printSchema()


# Cast the value column to String type (Kafka messages are typically in binary format)
orders_df = orders_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# Define a simple query to display the messages in console
query = orders_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()

