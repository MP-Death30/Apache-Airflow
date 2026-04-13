from __future__ import annotations
import logging
import os
import tempfile
import zipfile
import io
from datetime import datetime, timedelta
import pandas as pd
import requests

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.baseoperator import chain
from airflow.utils.dates import days_ago

from helpers.webhdfs_client import WebHDFSClient

logger = logging.getLogger(__name__)

DVF_URL = "https://www.data.gouv.fr/api/1/datasets/r/902db087-b0eb-4cbb-a968-0b499bde5bc4"
HDFS_RAW_PATH = "/data/dvf/raw"
POSTGRES_CONN_ID = "dvf_postgres"

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="pipeline_dvf_immobilier",
    description="ETL DVF: téléchargement -> HDFS raw -> PostgreSQL curated",
    schedule_interval="0 6 * * *",
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["dvf", "immobilier", "etl", "hdfs", "postgresql"],
)
def pipeline_dvf():

    @task(task_id="verifier_sources")
    def verifier_sources() -> dict:
        statuts = {}
        try:
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
            resp_dvf = requests.get(DVF_URL, stream=True, timeout=15, headers=headers)
            statuts["dvf_api"] = resp_dvf.status_code in (200, 302, 307)
            resp_dvf.close()
        except Exception:
            statuts["dvf_api"] = False

    @task(task_id="telecharger_dvf")
    def telecharger_dvf(statuts: dict) -> str:
        annee = datetime.now().year
        temp_dir = tempfile.gettempdir()
        zip_path = os.path.join(temp_dir, f"dvf_{annee}.zip")

        # 1. Téléchargement de l'archive ZIP
        with requests.get(DVF_URL, stream=True, timeout=300, headers={"User-Agent": "Mozilla/5.0"}) as r:
            r.raise_for_status()
            with open(zip_path, 'wb') as f:
                downloaded = 0
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
                    downloaded += len(chunk)
                    if downloaded % (50 * 1024 * 1024) == 0:
                        logger.info(f"Progression téléchargement : {downloaded / 1024 / 1024:.0f} Mo")

        # 2. Extraction du fichier TXT
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            liste_fichiers = zip_ref.namelist()
            # Identification dynamique du fichier texte dans l'archive
            fichier_txt = [f for f in liste_fichiers if f.endswith('.txt')][0]
            zip_ref.extract(fichier_txt, temp_dir)
            local_path = os.path.join(temp_dir, fichier_txt)

        # 3. Nettoyage de l'archive pour libérer l'espace du worker
        os.remove(zip_path)
        
        taille = os.path.getsize(local_path)
        logger.info(f"Extraction terminée : {fichier_txt} ({taille} octets)")
        
        return local_path

    @task(task_id="stocker_hdfs_raw")
    def stocker_hdfs_raw(local_path: str) -> str:
        hdfs_client = WebHDFSClient()
        
        # Ingestion locale pour partitionnement
        df = pd.read_csv(local_path, sep='|', decimal=',', encoding='utf-8', low_memory=False)
        df.columns = df.columns.str.lower().str.replace(' ', '_')
        
        # Extraction stricte des axes de partition
        df['annee_partition'] = pd.to_datetime(df['date_mutation'], format='%d/%m/%Y', errors='coerce').dt.year
        df['annee_partition'] = df['annee_partition'].fillna(datetime.now().year).astype(int)
        df['dept_partition'] = df['code_departement'].astype(str).str.zfill(2)
        
        groupes = df.groupby(['annee_partition', 'dept_partition'])
        chemins_crees = 0
        
        for (annee, dept), sous_df in groupes:
            if pd.isna(annee) or pd.isna(dept) or dept == 'nan':
                continue
                
            dossier_partition = f"{HDFS_RAW_PATH}/annee={annee}/dept={dept}"
            hdfs_client.mkdirs(dossier_partition)
            
            chemin_hdfs = f"{dossier_partition}/dvf_{annee}_{dept}.csv"
            chemin_temp_local = f"{local_path}_{annee}_{dept}.csv"
            
            # Standardisation en sortie : suppression du séparateur décimal français (',') au profit du standard ('.')
            sous_df.drop(columns=['annee_partition', 'dept_partition']).to_csv(chemin_temp_local, sep='|', index=False)
            
            hdfs_client.upload(chemin_hdfs, chemin_temp_local)
            os.remove(chemin_temp_local)
            chemins_crees += 1

        os.remove(local_path)
        logger.info(f"Partitionnement terminé : {chemins_crees} partitions créées dans {HDFS_RAW_PATH}")
        
        return HDFS_RAW_PATH

    @task(task_id="traiter_donnees")
    def traiter_donnees(hdfs_base_path: str) -> dict:
        hdfs_client = WebHDFSClient()
        
        try:
            elements_hdfs = hdfs_client.list_status(hdfs_base_path)
            annees_trouvees = [int(e.get("pathSuffix").split("=")[1]) for e in elements_hdfs if e.get("pathSuffix", "").startswith("annee=")]
            annee_cible = max(annees_trouvees)
        except Exception as e:
            raise RuntimeError(f"Échec détection de l'année : {e}")

        dept_cible = "75"
        chemin_partition = f"{hdfs_base_path}/annee={annee_cible}/dept={dept_cible}/dvf_{annee_cible}_{dept_cible}.csv"
        csv_bytes = hdfs_client.open(chemin_partition)
        df = pd.read_csv(io.BytesIO(csv_bytes), sep='|', low_memory=False)

        # Extraction temporelle vectorisée sur la date de vente réelle
        df['date_mutation'] = pd.to_datetime(df['date_mutation'], format='%d/%m/%Y', errors='coerce')
        df = df.dropna(subset=['date_mutation'])
        df['annee_mutation'] = df['date_mutation'].dt.year.astype(int)
        df['mois_mutation'] = df['date_mutation'].dt.month.astype(int)

        # Filtrage
        df = df[df['type_local'] == 'Appartement']
        df = df[df['nature_mutation'] == 'Vente']
        df = df[df['code_postal'].astype(str).str.startswith('750')]
        df = df[(df['surface_reelle_bati'] >= 9) & (df['surface_reelle_bati'] <= 500)]
        df = df[df['valeur_fonciere'] > 10000]

        df['arrondissement'] = df['code_postal'].astype(float).astype(int) % 100
        df['prix_m2'] = df['valeur_fonciere'] / df['surface_reelle_bati']

        # Agrégation multidimensionnelle : par année, mois et arrondissement
        agregats_df = df.groupby(['annee_mutation', 'mois_mutation', 'arrondissement']).agg(
            code_postal=('code_postal', 'first'),
            prix_m2_moyen=('prix_m2', 'mean'),
            prix_m2_median=('prix_m2', 'median'),
            prix_m2_min=('prix_m2', 'min'),
            prix_m2_max=('prix_m2', 'max'),
            nb_transactions=('prix_m2', 'count'),
            surface_moyenne=('surface_reelle_bati', 'mean')
        ).reset_index()

        # Statistiques globales macro : par année et mois
        stats_df = df.groupby(['annee_mutation', 'mois_mutation']).agg(
            nb_transactions_total=('valeur_fonciere', 'count'),
            prix_m2_median_paris=('prix_m2', 'median'),
            prix_m2_moyen_paris=('prix_m2', 'mean'),
            surface_mediane=('surface_reelle_bati', 'median')
        ).reset_index()

        idx_max = agregats_df.groupby(['annee_mutation', 'mois_mutation'])['prix_m2_median'].idxmax()
        idx_min = agregats_df.groupby(['annee_mutation', 'mois_mutation'])['prix_m2_median'].idxmin()

        stats_df['arrdt_plus_cher'] = agregats_df.loc[idx_max]['arrondissement'].values
        stats_df['arrdt_moins_cher'] = agregats_df.loc[idx_min]['arrondissement'].values

        return {
            "agregats": agregats_df.to_dict('records'),
            "stats_globales": stats_df.to_dict('records'),
            "annee_donnees": int(stats_df['annee_mutation'].max()),
            "mois_donnees": int(stats_df[stats_df['annee_mutation'] == stats_df['annee_mutation'].max()]['mois_mutation'].max())
        }

    @task(task_id="inserer_postgresql")
    def inserer_postgresql(resultats: dict) -> dict:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        agregats = resultats.get("agregats", [])
        stats = resultats.get("stats_globales", [])
        
        upsert_agregats = """
            INSERT INTO prix_m2_arrondissement
            (code_postal, arrondissement, annee, mois, prix_m2_moyen, prix_m2_median, prix_m2_min, prix_m2_max, nb_transactions, surface_moyenne, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (code_postal, annee, mois) DO UPDATE SET
            prix_m2_moyen = EXCLUDED.prix_m2_moyen,
            prix_m2_median = EXCLUDED.prix_m2_median,
            prix_m2_min = EXCLUDED.prix_m2_min,
            prix_m2_max = EXCLUDED.prix_m2_max,
            nb_transactions = EXCLUDED.nb_transactions,
            surface_moyenne = EXCLUDED.surface_moyenne,
            updated_at = NOW();
        """
        
        for arr in agregats:
            hook.run(upsert_agregats, parameters=(
                str(arr['code_postal']), int(arr['arrondissement']), int(arr['annee_mutation']), int(arr['mois_mutation']),
                float(arr['prix_m2_moyen']), float(arr['prix_m2_median']), float(arr['prix_m2_min']), float(arr['prix_m2_max']),
                int(arr['nb_transactions']), float(arr['surface_moyenne'])
            ))

        upsert_stats = """
            INSERT INTO stats_marche
            (annee, mois, nb_transactions_total, prix_m2_median_paris, prix_m2_moyen_paris, arrdt_plus_cher, arrdt_moins_cher, surface_mediane, date_calcul)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (annee, mois) DO UPDATE SET
            nb_transactions_total = EXCLUDED.nb_transactions_total,
            prix_m2_median_paris = EXCLUDED.prix_m2_median_paris,
            prix_m2_moyen_paris = EXCLUDED.prix_m2_moyen_paris,
            arrdt_plus_cher = EXCLUDED.arrdt_plus_cher,
            arrdt_moins_cher = EXCLUDED.arrdt_moins_cher,
            surface_mediane = EXCLUDED.surface_mediane,
            date_calcul = NOW();
        """

        for st in stats:
            hook.run(upsert_stats, parameters=(
                int(st['annee_mutation']), int(st['mois_mutation']), int(st['nb_transactions_total']),
                float(st['prix_m2_median_paris']), float(st['prix_m2_moyen_paris']),
                int(st['arrdt_plus_cher']), int(st['arrdt_moins_cher']), float(st['surface_mediane'])
            ))

        return {"annee_donnees": resultats.get("annee_donnees"), "mois_donnees": resultats.get("mois_donnees")}

    @task(task_id="generer_rapport")
    def generer_rapport(contexte: dict) -> str:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        annee = contexte.get("annee_donnees")
        mois = contexte.get("mois_donnees")
        
        query = """
            SELECT arrondissement, prix_m2_median, prix_m2_moyen, nb_transactions, surface_moyenne
            FROM prix_m2_arrondissement
            WHERE annee = %s AND mois = %s
            ORDER BY prix_m2_median DESC
            LIMIT 20;
        """
        records = hook.get_records(query, parameters=(annee, mois))
        
        rapport = "Arrondissement | Median (EUR/m2) | Moyen (EUR/m2) | Transactions\n"
        rapport += "-" * 65 + "\n"
        for r in records:
            rapport += f"{r[0]:>14} | {r[1]:>15.0f} | {r[2]:>14.0f} | {r[3]:>12}\n"
            
        logger.info(f"\n{rapport}")
        return rapport
    
    @task(task_id="analyser_tendances")
    def analyser_tendances(rapport_genere: str, contexte: dict) -> str:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        hook.run("ALTER TABLE stats_marche ADD COLUMN IF NOT EXISTS variation_globale_pct NUMERIC(5,2);")

        # Exécution du calcul d'évolution globale sur l'intégralité du dataset persistant
        query_update_stats = """
            WITH evolution_globale AS (
                SELECT
                    a.annee,
                    a.mois,
                    ROUND(
                        ((a.prix_m2_median_paris - b.prix_m2_median_paris) / b.prix_m2_median_paris) * 100,
                        2
                    ) AS variation
                FROM stats_marche a
                JOIN stats_marche b
                    ON (a.annee = b.annee AND a.mois = b.mois + 1)
                       OR (a.annee = b.annee + 1 AND a.mois = 1 AND b.mois = 12)
            )
            UPDATE stats_marche s
            SET variation_globale_pct = e.variation
            FROM evolution_globale e
            WHERE s.annee = e.annee AND s.mois = e.mois;
        """
        hook.run(query_update_stats)

        return "Tendances recalculées sur l'intégralité de l'historique DVF."

    t_verif = verifier_sources()
    t_download = telecharger_dvf(t_verif)
    t_hdfs = stocker_hdfs_raw(t_download)
    t_traiter = traiter_donnees(t_hdfs)
    
    # Rappel du Bonus 1 : t_traiter renvoie un dict contenant les données ET l'année détectée
    t_pg = inserer_postgresql(t_traiter)
    t_rapport = generer_rapport(t_traiter)
    t_tendances = analyser_tendances(t_rapport, t_traiter)

    # Enchaînement final
    chain(t_verif, t_download, t_hdfs, t_traiter, t_pg, t_rapport, t_tendances)

pipeline_dvf()