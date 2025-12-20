from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="load_etl_users",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["etl", "load"],
) as dag:

    load_data = PostgresOperator(
        task_id="insert_sample_data",
        postgres_conn_id="postgres_default",
        sql="""
        INSERT INTO etl_users (id, name, email)
        VALUES
        (1, 'Alice', 'alice@example.com'),
        (2, 'Bob', 'bob@example.com'),
        (3, 'Charlie', 'charlie@example.com')
        ON CONFLICT (id) DO NOTHING;
        """
    )
