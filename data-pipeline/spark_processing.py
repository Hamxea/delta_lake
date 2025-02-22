import logging
import psycopg2
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

# TODO: Dynamic streaming dataframe
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

def create_table_if_not_exists(cursor, table_name, schema):
    """
    Create the table in PostgreSQL if it doesn't exist.
    
    :param cursor: PostgreSQL cursor object.
    :param table_name: Name of the table to create.
    :param schema: StructType schema of the DataFrame.
    :return: None
    """
    cursor.execute(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{table_name}');")
    exists = cursor.fetchone()[0]

    if not exists:
        # Construct CREATE TABLE statement from DataFrame schema
        columns = []
        for field in schema:
            postgres_type = "VARCHAR"
            if isinstance(field.dataType, IntegerType):
                postgres_type = "INT"
            elif isinstance(field.dataType, FloatType):
                postgres_type = "FLOAT"
            elif isinstance(field.dataType, StringType):
                postgres_type = "VARCHAR"
            # Add more type mappings if necessary

            columns.append(f"{field.name} {postgres_type}")

        create_table_query = f"CREATE TABLE {table_name} ({', '.join(columns)});"
        cursor.execute(create_table_query)
        logger.info(f"Table '{table_name}' created successfully.")

def write_batch_to_postgres(df, epoch_id, url, properties, table_name):
    """
    Write each micro-batch of the streaming dataframe to PostgreSQL.
    
    :param df: Micro-batch DataFrame.
    :param epoch_id: Unique ID for each batch.
    :param url: JDBC URL for PostgreSQL.
    :param properties: Connection properties including user and password.
    :param table_name: Name of the PostgreSQL table.
    :return: None
    """
    try:
        # Establish a connection to PostgreSQL to check and create the table
        connection = psycopg2.connect(
            dbname=properties['database'],
            user=properties['user'],
            password=properties['password'],
            host=url.split(':')[2].split("//")[1],  # Extracting host from JDBC URL
            port=url.split(':')[3].split('/')[0]   # Extracting port from JDBC URL
        )
        cursor = connection.cursor()

        # Create table if it doesn't exist
        create_table_if_not_exists(cursor, table_name, df.schema)

        # Commit and close the connection
        connection.commit()
        cursor.close()
        connection.close()

        # Write the DataFrame batch to PostgreSQL
        df.write \
          .jdbc(url=url, table=table_name, mode="append", properties=properties)
        logger.info(f"Batch data written to PostgreSQL table '{table_name}' successfully.")

    except Exception as e:
        logger.error(f"Failed to write batch data to PostgreSQL. Error: {e}")
        raise



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
    path = "s3a://datalake/processed_data/deltatable/table1/"  # MinIO bucket path for output data
    checkpoint_location = "s3a://datalake/checkpoints/spark_metadata"  # Correct checkpoint location path
    
    
    # Database connection properties
    db_name = "postgres"
    user = "postgres"
    pwd = "postgres"   
    jdbc_url = "jdbc:postgresql://localhost:5432/postgres"  # Adjust hostname, port, and database name as needed
    connection_properties = {
        "user": user,
        "password": pwd,  # Replace with actual password
        "driver": "org.postgresql.Driver",
        "database": db_name  # Database name
    }
    
    table_name = "user_data"  # Name of the table in PostgreSQL

    spark = initialize_spark_session(app_name, access_key, secret_key, minio_endpoint)
    if spark:
        df = get_streaming_dataframe(spark, brokers, topic)
        if df:
            transformed_df = transform_streaming_data(df)
            
            # stream and save data to bucket minio 
            initiate_streaming_to_bucket(transformed_df, path, checkpoint_location)

                        
            # Write streaming data to PostgreSQL using foreachBatch
            #query = transformed_df.writeStream \
             #   .foreachBatch(lambda batch_df, epoch_id: write_batch_to_postgres(batch_df, epoch_id, jdbc_url, connection_properties, table_name)) \
              #  .outputMode("append") \
               # .start()

            #query.awaitTermination()


# Execute the main function if this script is run as the main module
if __name__ == '__main__':
    main()
