from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def get_current_date():
    return str(datetime.now().date())

with DAG(
    dag_id="exercice_jour1",
    schedule_interval=None,
    start_date=datetime(2026, 4, 8),
    catchup=False
) as dag:

    tache_debut = BashOperator(
        task_id="debut",
        bash_command='echo "Début du workflow"'
    )

    tache_date = PythonOperator(
        task_id="date_du_jour",
        python_callable=get_current_date
    )

    tache_fin = BashOperator(
        task_id="fin",
        bash_command='echo "Fin du workflow"'
    )

    tache_debut >> tache_date >> tache_fin