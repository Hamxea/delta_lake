import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# Initialize logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
logger = logging.getLogger("spark_structured_streaming")


def initialize_spark_session(app_name, access_key, secret_key, minio_endpoint):
    """
    Initialize the Spark Session with MinIO configurations and Delta Lake support.
    
    :param app_name: Name of the spark application.
    :param access_key: Access key for MinIO.
    :param secret_key: Secret key for MinIO.
    :param minio_endpoint: MinIO server endpoint (including port).
    :return: Spark session object or None if there's an error.
    """
    try:
        spark = SparkSession \
                .builder \
                .appName(app_name) \
                .config("spark.hadoop.fs.s3a.access.key", access_key) \
                .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
                .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
                .config("spark.hadoop.fs.s3a.path.style.access", "true") \
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")
        logger.info('Spark session initialized successfully with MinIO and Delta Lake configuration')
        return spark

    except Exception as e:
        logger.error(f"Spark session initialization failed. Error: {e}")
        return None


def get_streaming_dataframe(spark, brokers, topic):
    """
    Get a streaming dataframe from Kafka.
    
    :param spark: Initialized Spark session.
    :param brokers: Comma-separated list of Kafka brokers.
    :param topic: Kafka topic to subscribe to.
    :return: Dataframe object or None if there's an error.
    """
    try:
        df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", brokers) \
            .option("subscribe", topic) \
            .option("delimiter", ",") \
            .option("startingOffsets", "earliest") \
            .load()
        logger.info("Streaming dataframe fetched successfully")
        return df

    except Exception as e:
        logger.warning(f"Failed to fetch streaming dataframe. Error: {e}")
        return None


def transform_streaming_data(df):
    """
    Transform the initial dataframe to get the final structure.
    
    :param df: Initial dataframe with raw data.
    :return: Transformed dataframe.
    """
    schema = StructType([
        StructField("full_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("location", StringType(), False),
        StructField("city", StringType(), False),
        StructField("country", StringType(), False),
        StructField("postcode", IntegerType(), False),
        StructField("latitude", FloatType(), False),
        StructField("longitude", FloatType(), False),
        StructField("email", StringType(), False)
    ])

    transformed_df = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")
    logger.info("Streamed dataframe transformed successfully")
    return transformed_df


def initiate_streaming_to_bucket(df, path, checkpoint_location):
    """
    Start streaming the transformed data to the specified MinIO bucket in Delta Lake format.
    
    :param df: Transformed dataframe.
    :param path: MinIO bucket path.
    :param checkpoint_location: Checkpoint location for streaming (should be a valid S3/MinIO path).
    :return: None
    """
    logger.info("Initiating streaming process...")
    try:
        stream_query = (df.writeStream
                        .format("delta")
                        .outputMode("append")
                        .option("path", path)
                        .option("checkpointLocation", checkpoint_location)
                        .start())
        stream_query.awaitTermination()
    except Exception as e:
        logger.error(f"Failed to initiate streaming process. Error: {e}")
        raise


def main():
    app_name = "SparkStructuredStreamingToMinIO"
    access_key = "1ocIs2hDgDdzTrEeMRUe"
    secret_key = "uWcPmBGkdBm4Xi992NpofSHbeEebtiXrNyYjdvn8"
    minio_endpoint = "http://minio:9000"
    brokers = "kafka_broker_1:29092,kafka_broker_2:19093,kafka_broker_3:19094"
    topic = "names_topic"
    path = "s3a://datalake/processed_data/deltatable"  # MinIO bucket path for output data
    checkpoint_location = "s3a://datalake/checkpoints/spark_metadata"  # Correct checkpoint location path

    spark = initialize_spark_session(app_name, access_key, secret_key, minio_endpoint)
    if spark:
        df = get_streaming_dataframe(spark, brokers, topic)
        if df:
            transformed_df = transform_streaming_data(df)
            initiate_streaming_to_bucket(transformed_df, path, checkpoint_location)


# Execute the main function if this script is run as the main module
if __name__ == '__main__':
    main()
