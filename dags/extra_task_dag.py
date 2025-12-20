from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import pandas as pd
import psycopg2
import json
import os

# -------------------- FILE PATHS --------------------
BASE_PATH = "/opt/airflow"
RAW_FILE = f"{BASE_PATH}/users_raw.json"
TRANSFORMED_FILE = f"{BASE_PATH}/users_transformed.json"

RESULTS_FOLDER = "/tmp/airflow_parallel_results"
os.makedirs(RESULTS_FOLDER, exist_ok=True)

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

# -------------------- PARALLEL TASKS --------------------
def sum_of_squares(task_id: str, n: int):
    result = sum(i ** 2 for i in range(1, n + 1))
    file_path = os.path.join(RESULTS_FOLDER, f"{task_id}.txt")
    with open(file_path, "w") as f:
        f.write(str(result))
    return result

def aggregate_results():
    total = 0

    # ONLY read parallel task outputs
    for file in os.listdir(RESULTS_FOLDER):
        if file.startswith("sum_squares_"):
            with open(os.path.join(RESULTS_FOLDER, file)) as f:
                total += int(f.read())

    with open(os.path.join(RESULTS_FOLDER, "final_total.txt"), "w") as f:
        f.write(str(total))

# -------------------- DAG DEFINITION --------------------
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 12, 1),
    "retries": 1,
}

with DAG(
    dag_id="etl_with_parallel_computation",
    default_args=default_args,
    schedule_interval="@daily",
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

    # Create parallel computation tasks
    parallel_tasks = []
    for i, n in enumerate([1000, 2000, 3000, 4000, 5000], start=1):
        t = PythonOperator(
            task_id=f"sum_squares_{i}",
            python_callable=sum_of_squares,
            op_kwargs={"task_id": f"sum_squares_{i}", "n": n},
        )
        parallel_tasks.append(t)

    aggregate_task = PythonOperator(
        task_id="aggregate_results",
        python_callable=aggregate_results,
    )

    # -------------------- DEPENDENCIES --------------------
    extract_task >> transform_task >> load_task
    load_task >> parallel_tasks >> aggregate_task
