# 2. Communication XCom
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def generer_liste():
    return [5, 10, 15, 20, 25]

def calculer_somme(ti):
    liste = ti.xcom_pull(task_ids="generer_liste")
    return sum(liste)

def afficher_resultat(ti):
    somme = ti.xcom_pull(task_ids="calculer_somme")
    print(f"Somme finale calculée : {somme}")

with DAG(
    dag_id="exo2_xcom",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
) as dag2:

    t1 = PythonOperator(
        task_id="generer_liste",
        python_callable=generer_liste
    )

    t2 = PythonOperator(
        task_id="calculer_somme",
        python_callable=calculer_somme
    )

    t3 = PythonOperator(
        task_id="afficher_resultat",
        python_callable=afficher_resultat
    )

    t1 >> t2 >> t3