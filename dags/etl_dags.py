from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import pandas as pd
import psycopg2
import json
import os

# File paths inside container
BASE_PATH = "/opt/airflow"
RAW_FILE = f"{BASE_PATH}/users_raw.json"
TRANSFORMED_FILE = f"{BASE_PATH}/users_transformed.json"

# -------------------- TASK 1: EXTRACT --------------------
def extract():
    response = requests.get("https://jsonplaceholder.typicode.com/users")
    response.raise_for_status()

    with open(RAW_FILE, "w") as f:
        json.dump(response.json(), f)

# -------------------- TASK 2: TRANSFORM --------------------
def transform():
    with open(RAW_FILE, "r") as f:
        data = json.load(f)

    df = pd.DataFrame(data)[["id", "name", "username", "email"]]

    with open(TRANSFORMED_FILE, "w") as f:
        json.dump(df.to_dict(orient="records"), f)

# -------------------- TASK 3: LOAD --------------------
def load():
    conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow",
        port=5432,
    )
    cursor = conn.cursor()

    with open(TRANSFORMED_FILE, "r") as f:
        data = json.load(f)

    for row in data:
        cursor.execute(
            """
            INSERT INTO etl_users (id, name, username, email)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                username = EXCLUDED.username,
                email = EXCLUDED.email
            """,
            (row["id"], row["name"], row["username"], row["email"]),
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
    dag_id="etl_dags_postgres",
    default_args=default_args,
   schedule_interval='@daily',
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform,
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=load,
    )

    # TASK DEPENDENCIES
    extract_task >> transform_task >> load_task
