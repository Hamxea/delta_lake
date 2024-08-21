# Use the official Apache Spark image as the base
FROM bitnami/spark:3.5.2

# Optionally add custom JARs or configurations
ADD https://repo1.maven.org/maven2/org/scala-lang/scala-library/2.12.17/scala-library-2.12.17.jar /opt/spark/jars/

# Example of adding a specific Kafka connector JAR
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.13/3.5.2/spark-sql-kafka-0-10_2.13-3.5.2.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector_2.13/3.5.1/spark-cassandra-connector_2.13-3.5.1.jar /opt/spark/jars/


# Any additional setup can go here
# spark-cassandra-connector_2.13-3.5.1
# spark-sql-kafka-0-10_2.13-3.5.2