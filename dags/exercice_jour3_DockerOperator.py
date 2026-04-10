from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.python import PythonOperator
from docker.types import Mount
from datetime import datetime
import logging
import json
import os

# Attention : Ce chemin doit exister sur la machine hôte physique, 
# pas uniquement dans le conteneur Airflow.
CHEMIN_HOTE = "/tmp/partage_docker"
CHEMIN_CONTENEUR = "/data"

def preparer_donnees():
    os.makedirs(CHEMIN_HOTE, exist_ok=True)
    
    # Génération du script qui sera exécuté à l'intérieur du conteneur Docker
    script_content = """import os
import json

msg = os.environ.get('MESSAGE_TEST', 'Valeur par défaut')
resultat = {'statut': 'succes', 'contexte': 'isolation_docker', 'message': msg}

with open('/data/output.json', 'w') as f:
    json.dump(resultat, f)
print("Traitement terminé. Fichier généré.")
"""
    with open(f"{CHEMIN_HOTE}/script_cible.py", "w") as f:
        f.write(script_content)

def valider_resultat():
    fichier_resultat = f"{CHEMIN_HOTE}/output.json"
    if not os.path.exists(fichier_resultat):
        raise FileNotFoundError(f"Échec. Fichier manquant : {fichier_resultat}")
        
    with open(fichier_resultat, "r") as f:
        donnees = json.load(f)
        
    logging.info(f"Lecture des données générées par le conteneur éphémère : {donnees}")

with DAG(
    dag_id="exercice_docker_operator",
    start_date=datetime(2026, 4, 10),
    schedule=None,
    catchup=False
) as dag:

    t_preparer = PythonOperator(
        task_id="preparer_script_python",
        python_callable=preparer_donnees
    )

    t_docker = DockerOperator(
        task_id="executer_python_isole",
        image="python:3.9-slim",
        api_version="auto",
        auto_remove="force",
        command="python /data/script_cible.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        environment={
            "MESSAGE_TEST": "Transmission de variable réussie"
        },
        mounts=[
            Mount(
                source=CHEMIN_HOTE,
                target=CHEMIN_CONTENEUR,
                type="bind"
            )
        ]
    )

    t_valider = PythonOperator(
        task_id="recuperer_donnees_volume",
        python_callable=valider_resultat
    )

    t_preparer >> t_docker >> t_valider