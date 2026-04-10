from airflow.decorators import dag, task
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
from hdfs import InsecureClient
import logging

@dag(
    dag_id="logs_compaction_dag",
    schedule="0 3 * * 1",  # Lundi 03:00 UTC
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["hdfs", "compaction", "maintenance"],
)
def logs_compaction_dag():

    # Bonus : Synchronisation inter-DAG
    # Le DAG d'ingestion s'exécute à 02:00. La compaction à 03:00.
    # L'écart temporel d'exécution (execution_delta) est d'exactement 1 heure.
    attendre_ingestion = ExternalTaskSensor(
        task_id="attendre_ingestion_dimanche",
        external_dag_id="logs_ecommerce_dag",
        external_task_id="archiver_logs_hdfs",
        execution_delta=timedelta(hours=1),
        timeout=600,
        mode="reschedule"
    )

    @task()
    def lister_fichiers_semaine(**context) -> list:
        client = InsecureClient('http://namenode:9870')
        logical_date = context["data_interval_start"]
        chemin_raw = "/data/ecommerce/logs/raw"
        
        # Génération des 7 dates de la semaine précédente (Lundi à Dimanche)
        dates_cibles = [(logical_date - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(1, 8)]
        noms_attendus = [f"access_{d}.log" for d in dates_cibles]
        
        try:
            fichiers_hdfs = client.list(chemin_raw)
        except Exception as e:
            logging.warning(f"Répertoire indisponible ou vide : {e}")
            return []
            
        # Filtrage strict sur la fenêtre temporelle
        fichiers_valides = [
            f"{chemin_raw}/{f}" for f in fichiers_hdfs if f in noms_attendus
        ]
        
        logging.info(f"{len(fichiers_valides)} fichiers identifiés pour la compaction.")
        return fichiers_valides

    @task()
    def fusionner_fichiers(fichiers: list, **context) -> str:
        if not fichiers:
            logging.info("Aucun fichier à compacter. Arrêt de la branche.")
            return ""
            
        client = InsecureClient('http://namenode:9870')
        logical_date = context["data_interval_start"]
        
        # Calcul de la semaine ISO de la donnée (logical_date - 1 jour nous place dans la semaine traitée)
        date_ref = logical_date - timedelta(days=1)
        annee, semaine, _ = date_ref.isocalendar()
        
        chemin_weekly_dir = "/data/ecommerce/logs/weekly"
        chemin_weekly = f"{chemin_weekly_dir}/{annee}-W{semaine:02d}.log"
        
        client.makedirs(chemin_weekly_dir)
        
        contenu_fusionne = ""
        for fichier in fichiers:
            with client.read(fichier, encoding='utf-8') as reader:
                contenu_fusionne += reader.read()
                
        client.write(chemin_weekly, data=contenu_fusionne, encoding='utf-8', overwrite=True)
        logging.info(f"Fusion terminée : {chemin_weekly}")
        
        return chemin_weekly

    @task()
    def valider_compaction(chemin_weekly: str, fichiers_source: list) -> None:
        if not chemin_weekly:
            return
            
        client = InsecureClient('http://namenode:9870')
        
        lignes_sources = 0
        for fichier in fichiers_source:
            with client.read(fichier, encoding='utf-8') as reader:
                lignes_sources += len(reader.readlines())
                
        with client.read(chemin_weekly, encoding='utf-8') as reader:
            lignes_cible = len(reader.readlines())
            
        if lignes_sources != lignes_cible:
            raise ValueError(
                f"Échec d'intégrité : Sources ({lignes_sources} lignes) ≠ Cible ({lignes_cible} lignes)."
            )
            
        logging.info("Validation d'intégrité réussie. Zéro perte de données.")

    @task()
    def supprimer_fichiers_journaliers(fichiers: list, validation_requise: None) -> None:
        # L'argument `validation_requise` force la dépendance logique avec la tâche de validation
        client = InsecureClient('http://namenode:9870')
        for fichier in fichiers:
            client.delete(fichier)
            logging.info(f"Fichier purgé : {fichier}")

    # Définition du flux d'exécution via l'API TaskFlow
    fichiers_a_traiter = lister_fichiers_semaine()
    fichier_hebdomadaire = fusionner_fichiers(fichiers_a_traiter)
    validation_status = valider_compaction(fichier_hebdomadaire, fichiers_a_traiter)
    
    # Chaînage structurel
    attendre_ingestion >> fichiers_a_traiter
    supprimer_fichiers_journaliers(fichiers_a_traiter, validation_status)

# Instanciation du DAG
logs_compaction_dag()