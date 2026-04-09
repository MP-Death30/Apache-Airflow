# 3. Sensor personnalisé
from airflow import DAG
from airflow.sensors.python import PythonSensor
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import os

def verifier_fichier():
    return os.path.exists("/tmp/go.txt")

with DAG(
    dag_id="exo3_sensor",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
) as dag3:

    attente = PythonSensor(
        task_id="attendre_fichier",
        python_callable=verifier_fichier,
        mode="reschedule"
    )

    suite = EmptyOperator(task_id="suite_workflow")

    attente >> suite