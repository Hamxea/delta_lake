#!/bin/bash
set -e

# Install Python dependencies
if [ -e "/opt/airflow/requirements.txt" ]; then
  $(command python) pip install --upgrade pip
  $(command -v pip) install --user -r requirements.txt
fi

# Initialize Airflow database
if [ ! -f "/opt/airflow/airflow.db" ]; then
  airflow db init && \
  airflow users create \
    --username admin \
    --firstname admin \
    --lastname admin \
    --role Admin \
    --email admin@example.com \
    --password admin
fi

# Upgrade the Airflow database
$(command -v airflow) db upgrade

# Initialize PostgreSQL if required (Optional)
# Example: Run a script to set up the database schema
# psql -U postgres -d mydatabase -a -f /path/to/init.sql

# Start the Airflow webserver
exec airflow webserver
