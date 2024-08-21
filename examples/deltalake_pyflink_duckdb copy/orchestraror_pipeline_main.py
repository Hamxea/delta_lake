import subprocess

def run_data_generation():
    subprocess.run(["python", "generate_data.py"])

def run_duckdb_queries():
    subprocess.run(["python", "duckdb_queries.py"])

def run_flink_job():
    subprocess.run(["python", "flink_job.py"])

if __name__ == "__main__":
    print("Starting data generation...")
    run_data_generation()
    print("Data generation completed.")
    
    print("Running DuckDB queries...")
    run_duckdb_queries()
    print("DuckDB queries completed.")
    
    print("Starting Flink job...")
    run_flink_job()
    print("Flink job completed.")
