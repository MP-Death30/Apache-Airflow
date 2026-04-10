from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.apache.hdfs.sensors.web_hdfs import WebHdfsSensor
from airflow.utils.trigger_rule import TriggerRule
from hdfs_sensor import HdfsFileSensor
from datetime import datetime
from hdfs import InsecureClient
from collections import Counter
import subprocess
import logging
import os
import re

SEUIL_ERREUR_PCT = 5.0 # Seuil d'alerte : 5% d'erreurs HTTP

def generer_logs_journaliers(**context):
    """
    Génère 1000 lignes de logs Apache pour la date d'exécution du DAG.
    Sauvegarde dans /tmp/access_.log.
    Retourne le chemin du fichier (stocké dans XCom pour les tâches suivantes).
    """
    # La date logique d'exécution (pas forcément aujourd'hui si catchup=True)
    execution_date = context["ds"] # Format YYYY-MM-DD
    fichier_sortie = f"/tmp/access_{execution_date}.log"
    script_path = "/opt/airflow/scripts/generer_logs.py"
    
    # À compléter :
    # 1. Utiliser subprocess.run() pour appeler le script Python :
    # ["python3", script_path, execution_date, "1000", fichier_sortie]
    # 2. Passer check=True pour lever une exception si le script échoue
    # 3. Vérifier que le fichier existe et logger sa taille en octets
    # 4. Retourner fichier_sortie
    commande = ["python3", script_path, execution_date, "1000", fichier_sortie]
    subprocess.run(commande, check=True, capture_output=True, text=True)
    
    if not os.path.exists(fichier_sortie):
        raise FileNotFoundError(f"Fichier absent : {fichier_sortie}")
        
    taille = os.path.getsize(fichier_sortie)
    logging.info(f"Fichier généré : {fichier_sortie} | Taille : {taille} octets")
    return fichier_sortie

def uploader_vers_hdfs(**context):
    # Copie du fichier depuis le conteneur airflow-scheduler
    # vers le conteneur namenode (via volume partagé ou commande docker)
    # À compléter : utiliser la commande hdfs dfs -put appropriée
    # Indice : docker exec namenode hdfs dfs -put  
    # Attention : le fichier /tmp/ n'est pas directement accessible depuis namenode
    # Solution : API WebHDFS via InsecureClient (contourne l'impossibilité du socket Docker)
    execution_date = context["ds"]
    fichier_local = f"/tmp/access_{execution_date}.log"
    chemin_hdfs_dir = "/data/ecommerce/logs/raw"
    chemin_hdfs = f"{chemin_hdfs_dir}/access_{execution_date}.log"
    
    client = InsecureClient('http://namenode:9870')
    client.makedirs(chemin_hdfs_dir)
    client.upload(chemin_hdfs, fichier_local, overwrite=True)
    logging.info(f"Upload terminé vers {chemin_hdfs}")

def analyser_logs(**context):
    execution_date = context["ds"]
    chemin_hdfs = f"/data/ecommerce/logs/raw/access_{execution_date}.log"
    
    client = InsecureClient('http://namenode:9870')
    with client.read(chemin_hdfs, encoding='utf-8') as reader:
        lignes = reader.readlines()

    total = len(lignes)
    if total == 0:
        logging.warning("Fichier vide.")
        return 0.0

    status_counter = Counter()
    url_counter = Counter()
    erreurs = 0
    
    # Regex pour extraire l'URL et le Code Statut HTTP
    # Capture le groupe 1 (URL) et le groupe 2 (Statut)
    pattern = re.compile(r'"(?:GET|POST) (.*?) HTTP/[0-9.]+" (\d{3})')
    
    for ligne in lignes:
        match = pattern.search(ligne)
        if match:
            url = match.group(1)
            status = match.group(2)
            
            # Incrémentation des compteurs
            status_counter[status] += 1
            url_counter[url] += 1
            
            # Comptage des erreurs (codes 4xx et 5xx)
            if status.startswith('4') or status.startswith('5'):
                erreurs += 1

    # --- AFFICHAGE DANS LES LOGS AIRFLOW (Exigence du TP) ---
    
    logging.info("\n=== STATUS CODES ===")
    # most_common() trie automatiquement par ordre décroissant
    for status, count in status_counter.most_common():
        logging.info(f"{count:>5} requêtes avec le statut {status}")

    logging.info("\n=== TOP 5 URLS ===")
    for url, count in url_counter.most_common(5):
        logging.info(f"{count:>5} visites : {url}")
        
    taux_erreur = (erreurs / total) * 100
    logging.info("\n=== TAUX ERREUR ===")
    logging.info(f"Total: {total} | Erreurs: {erreurs} | Taux: {taux_erreur:.2f}%\n")
    
    # On retourne toujours le taux pour le BranchPythonOperator
    return taux_erreur

def brancher_selon_taux_erreur(**context):
    """
    Lit le taux d'erreur calculé par analyser_logs_hdfs.
    Retourne le task_id de la branche à exécuter.
    """
    # À compléter :
    # 1. Lire le fichier /tmp/taux_erreur_.txt (Remplacé par transmission XCom)
    # 2. Parser les valeurs ERREURS et TOTAL (Géré dans analyser_logs)
    # 3. Calculer le taux : taux_pct = (erreurs / total) * 100
    # 4. Logger le taux
    # 5. Retourner "alerter_equipe_ops" si taux > SEUIL_ERREUR_PCT
    # sinon retourner "archiver_rapport_ok"
    
    ti = context["ti"]
    taux = ti.xcom_pull(task_ids="analyser_logs_hdfs")
    
    logging.info(f"Taux d'erreur récupéré : {taux:.2f}%")
    
    if taux > SEUIL_ERREUR_PCT:
        return "alerter_equipe_ops"
    else:
        return "archiver_rapport_ok"

def alerter_equipe_ops(**context):
    """Simule l'envoi d'une alerte à l'équipe Ops (Slack, PagerDuty, etc.)."""
    execution_date = context["ds"]
    logging.warning(
        f"[ALERTE] Taux d'erreur HTTP anormal détecté pour les logs du {execution_date}. "
        "Vérifiez les serveurs web."
    )
    # En production : appel API Slack, PagerDuty, ou envoi email via SMTP

def archiver_rapport_ok(**context):
    """Logue que tout est nominal et archive un rapport de bonne santé."""
    execution_date = context["ds"]
    logging.info(
        f"[OK] Taux d'erreur dans les seuils normaux pour les logs du {execution_date}."
    )

def archiver_logs_hdfs(**context):
    execution_date = context["ds"]
    chemin_raw = f"/data/ecommerce/logs/raw/access_{execution_date}.log"
    chemin_processed_dir = "/data/ecommerce/logs/processed"
    chemin_processed = f"{chemin_processed_dir}/access_{execution_date}.log"
    
    client = InsecureClient('http://namenode:9870')
    client.makedirs(chemin_processed_dir)
    
    # Ouverture d'un flux pour copier la donnée sans altérer la source
    logging.info(f"Début de la copie de {chemin_raw} vers {chemin_processed}")
    with client.read(chemin_raw) as reader:
        donnees = reader.read()
        client.write(chemin_processed, donnees, overwrite=True)
        
    logging.info("Copie terminée. La zone Raw reste intacte.")


with DAG(
    dag_id="logs_ecommerce_dag",
    start_date=datetime(2024, 1, 1),
    schedule="0 2 * * *",
    catchup=False
) as dag:

    t_generer = PythonOperator(
        task_id="generer_logs_journaliers",
        python_callable=generer_logs_journaliers
    )

    t_upload = PythonOperator(
        task_id="uploader_vers_hdfs",
        python_callable=uploader_vers_hdfs
    )

    t_sensor = HdfsFileSensor(
        task_id="attendre_fichier_hdfs",
        hdfs_path="/data/ecommerce/logs/raw/access_{{ ds }}.log",
        namenode_url="http://namenode:9870", # Substitution de la variable pour garantir l'exécution immédiate
        poke_interval=30,
        timeout=600,
        mode="reschedule",
    )

    t_analyser = PythonOperator(
        task_id="analyser_logs_hdfs",
        python_callable=analyser_logs
    )

    # Le BranchPythonOperator décide de la branche à emprunter selon le taux d’erreur calculé à l’étape précédente.
    t_branch = BranchPythonOperator(
        task_id="brancher_selon_taux_erreur",
        python_callable=brancher_selon_taux_erreur
    )

    t_alerte = PythonOperator(
        task_id="alerter_equipe_ops",
        python_callable=alerter_equipe_ops
    )

    t_archive_ok = PythonOperator(
        task_id="archiver_rapport_ok",
        python_callable=archiver_rapport_ok
    )

    # Point clé : La tâche finale archiver_logs_hdfs doit s’exécuter quelle que soit la branche choisie.
    # Utilisez trigger_rule="none_failed_min_one_success" sur cette tâche.
    t_archiver_final = PythonOperator(
        task_id="archiver_logs_hdfs",
        python_callable=archiver_logs_hdfs,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    t_generer >> t_upload >> t_sensor >> t_analyser >> t_branch
    
    t_branch >> [t_alerte, t_archive_ok] >> t_archiver_final