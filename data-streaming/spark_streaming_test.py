from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

def create_spark_connection():
    s_conn = None
    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .master('spark://172.19.0.3:7077') \
            .config('spark.jars.packages',
                    'org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,'
                    'com.datastax.spark:spark-cassandra-connector_2.12:3.4.1') \
            .config('spark.cassandra.connection.host', 'cassandra') \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")
    return s_conn

def create_kafka_dataframe(spark):
    kafka_df = None
    try:
        kafka_df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "broker:9092") \
            .option("subscribe", "users_created") \
            .option("startingOffsets", "earliest") \
            .load()

        logging.info("Kafka DataFrame created successfully!")
    except Exception as e:
        logging.error(f"Kafka DataFrame creation failed: {e}")
    return kafka_df

def create_selection_df_from_kafka(kafka_df):
    selection_df = None
    if kafka_df is not None:
        try:
            schema = StructType([
                StructField("key", StringType(), True),
                StructField("value", StringType(), True)
            ])

            selection_df = kafka_df.selectExpr("CAST(value AS STRING)") \
                                   .withColumn("value", from_json(col("value"), schema)) \
                                   .select(col("value.*"))

            logging.info("Selection DataFrame created successfully!")
        except Exception as e:
            logging.error(f"Failed to create selection DataFrame from Kafka DataFrame: {e}")
    else:
        logging.error("Kafka DataFrame is None, cannot create selection DataFrame.")
    return selection_df

if __name__ == "__main__":
    spark = create_spark_connection()

    if spark:
        kafka_df = create_kafka_dataframe(spark)
        selection_df = create_selection_df_from_kafka(kafka_df)

        if selection_df is not None:
            query = selection_df.writeStream \
                                .outputMode("append") \
                                .format("console") \
                                .start()

            query.awaitTermination()
