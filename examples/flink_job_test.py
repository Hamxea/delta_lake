import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

# Define the root path and data table path
root_path = "/Users/hamzaharunamohammed/Desktop/etiya/delta_lake"
data_table = root_path + "/data/deltalake/bss.delta"

# Add the JARs to the classpath
jars = [
     f"{root_path}/flink-connector-filesystem_2.12-1.11.5.jar",
    f"{root_path}/flink-parquet_2.12-1.13.2.jar"
]
os.environ['FLINK_CLASSPATH'] = ':'.join(jars)

# Initialize Flink environment
env = StreamExecutionEnvironment.get_execution_environment()
settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
t_env = StreamTableEnvironment.create(env, environment_settings=settings)

# Define Delta table source using SQL DDL
source_ddl = f"""
CREATE TABLE delta_source (
    caller STRING,
    receiver STRING,
    timestamp TIMESTAMP(3),
    duration INT,
    plan_type STRING,
    service_type STRING,
    cost DOUBLE
) WITH (
    'connector' = 'filesystem',
    'path' = '{data_table}',
    'format' = 'parquet'
)
"""
t_env.execute_sql(source_ddl)

# Define a simple transformation
result = t_env.sql_query("SELECT service_type, COUNT(*) as total_calls, SUM(cost) as total_cost FROM delta_source GROUP BY service_type")

# Define a sink
sink_ddl = """
CREATE TABLE print_sink (
    service_type STRING,
    total_calls BIGINT,
    total_cost DOUBLE
) WITH (
    'connector' = 'print'
)
"""
t_env.execute_sql(sink_ddl)

# Insert result into sink
result.execute_insert("print_sink").wait()

# Define another transformation for real-time analytics
real_time_query = t_env.sql_query("""
SELECT
    TUMBLE_START(timestamp, INTERVAL '1' HOUR) AS window_start,
    TUMBLE_END(timestamp, INTERVAL '1' HOUR) AS window_end,
    plan_type,
    COUNT(*) AS call_count,
    SUM(duration) AS total_duration,
    SUM(cost) AS total_cost
FROM delta_source
GROUP BY TUMBLE(timestamp, INTERVAL '1' HOUR), plan_type
""")

# Define another sink for real-time analytics
real_time_sink_ddl = """
CREATE TABLE real_time_sink (
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    plan_type STRING,
    call_count BIGINT,
    total_duration BIGINT,
    total_cost DOUBLE
) WITH (
    'connector' = 'print'
)
"""
t_env.execute_sql(real_time_sink_ddl)

# Insert real-time query result into sink
real_time_query.execute_insert("real_time_sink").wait()
