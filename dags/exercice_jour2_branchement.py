# 1. DAG avec branchement
from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime
import random

def determiner_branche():
    nombre = random.randint(1, 100)
    return "pair" if nombre % 2 == 0 else "impair"

with DAG(
    dag_id="exo1_branchement",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
) as dag1:

    branchement = BranchPythonOperator(
        task_id="branchement",
        python_callable=determiner_branche
    )

    tache_pair = EmptyOperator(task_id="pair")
    tache_impair = EmptyOperator(task_id="impair")

    tache_finale = EmptyOperator(
        task_id="finale",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    branchement >> [tache_pair, tache_impair] >> tache_finale