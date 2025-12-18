from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import psycopg2
import json
import os
import requests

# -------------------- CONFIG --------------------
BASE_PATH = "/opt/airflow"
RAW_FILE = f"{BASE_PATH}/raw_data.json"
TRANSFORMED_FILE = f"{BASE_PATH}/transformed_data.json"

# Set source type and path
SOURCE_TYPE = "csv"  # "api", "csv", "json"
SOURCE_PATH = f"{BASE_PATH}/data/products.csv"  # path to CSV/JSON or API URL
TABLE_NAME = "etl_dynamic_table"  # table name in Postgres

DB_CONFIG = {
    "host": "postgres",
    "database": "airflow",
    "user": "airflow",
    "password": "airflow",
    "port": 5432,
}

# -------------------- TASK 1: EXTRACT --------------------
def extract():
    if SOURCE_TYPE.lower() == "csv":
        data = pd.read_csv(SOURCE_PATH).to_dict(orient="records")
    elif SOURCE_TYPE.lower() == "json":
        with open(SOURCE_PATH, "r") as f:
            data = json.load(f)
    elif SOURCE_TYPE.lower() == "api":
        response = requests.get(SOURCE_PATH)
        response.raise_for_status()
        data = response.json()
    else:
        raise ValueError("Unsupported source type. Use 'api', 'csv', or 'json'.")

    with open(RAW_FILE, "w") as f:
        json.dump(data, f)

# -------------------- TASK 2: TRANSFORM --------------------
def transform():
    with open(RAW_FILE, "r") as f:
        data = json.load(f)

    df = pd.DataFrame(data)

    # Automatically detect primary key
    if "id" in df.columns:
        primary_key = "id"
    else:
        primary_key = df.columns[0]  # fallback to first column

    # Save transformed data and metadata
    with open(TRANSFORMED_FILE, "w") as f:
        json.dump({
            "data": df.to_dict(orient="records"),
            "columns": list(df.columns),
            "primary_key": primary_key
        }, f)

# -------------------- TASK 3: LOAD --------------------
def load():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    with open(TRANSFORMED_FILE, "r") as f:
        saved = json.load(f)

    data = saved["data"]
    columns = saved["columns"]
    primary_key = saved["primary_key"]

    if not data:
        conn.close()
        return

    # Dynamically create table if not exists
    col_defs = ", ".join([f"{col} TEXT" for col in columns])
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            {col_defs},
            PRIMARY KEY ({primary_key})
        );
    """)

    # Insert or update rows
    for row in data:
        cols = row.keys()
        vals = tuple(row[col] for col in cols)
        col_str = ", ".join(cols)
        val_placeholders = ", ".join(["%s"] * len(cols))
        update_str = ", ".join([f"{col}=EXCLUDED.{col}" for col in cols if col != primary_key])

        cursor.execute(
            f"""
            INSERT INTO {TABLE_NAME} ({col_str})
            VALUES ({val_placeholders})
            ON CONFLICT ({primary_key}) DO UPDATE SET {update_str}
            """,
            vals
        )

    conn.commit()
    cursor.close()
    conn.close()

# -------------------- DAG DEFINITION --------------------
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 12, 1),
    "retries": 1,
}

with DAG(
    dag_id="etl_dynamic_any_source",
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=load
    )

    extract_task >> transform_task >> load_task
