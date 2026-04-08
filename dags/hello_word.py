from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def print_hello():
    print("Hello from Airflow!")

with DAG(
    dag_id="premier_dag_fonctionnel",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2026, 4, 8),
    catchup=False
) as dag:

    tache_python = PythonOperator(
        task_id="dire_bonjour",
        python_callable=print_hello
    )