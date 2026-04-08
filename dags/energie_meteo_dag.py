from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pendulum
import requests
import logging
import json
from datetime import date

# Timezone Paris pour RTE
local_tz = pendulum.timezone("Europe/Paris")

# Coordonnées des 5 régions cibles
REGIONS = {
    "Île-de-France": {"lat": 48.8566, "lon": 2.3522},
    "Occitanie": {"lat": 43.6047, "lon": 1.4442},
    "Nouvelle-Aquitaine": {"lat": 44.8378, "lon": -0.5792},
    "Auvergne-Rhône-Alpes": {"lat": 45.7640, "lon": 4.8357},
    "Hauts-de-France": {"lat": 50.6292, "lon": 3.0573},
}

default_args = {
    "owner": "rte-data-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# --- Fonctions à implémenter (étapes 3 à 7) ---
def verifier_apis(**context):
    """
    Vérifie la disponibilité des APIs Open-Meteo et éCO2mix.
    Lève une exception si une API est indisponible pour bloquer le pipeline.
    """
    apis = {
        "Open-Meteo": (
        "https://api.open-meteo.com/v1/forecast"
        "?latitude=48.8566&longitude=2.3522"
        "&daily=sunshine_duration&timezone=Europe/Paris&forecast_days=1"
        ),
        "éCO2mix": (
            "https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets"
            "/eco2mix-regional-cons-def/records?limit=1&timezone=Europe%2FParis"
        ),
    }

    for nom, url in apis.items():
        try:
            response = requests.get(url, timeout=10)
            if response.status_code != 200:
                raise ValueError(f"Échec API {nom} : statut HTTP {response.status_code}.")
            
            logging.info(f"API {nom} opérationnelle (Code 200).")
            
        except requests.exceptions.RequestException as e:
            raise ValueError(f"Indisponibilité réseau API {nom} : {str(e)}")
    logging.info("Toutes les APIs sont disponibles. Pipeline autorisé à continuer.")

def collecter_meteo_regions(**context):
    """
    Collecte pour chaque région : durée d'ensoleillement (h) et vitesse max du vent (km/h)
    Retourne un dictionnaire {region: {ensoleillement_h: float, vent_kmh: float}}.
    Ce dictionnaire sera automatiquement stocké dans XCom via le return.
    """
    BASE_URL = "https://api.open-meteo.com/v1/forecast"
    resultats = {}
    
    for region, coords in REGIONS.items():
        params = {
            "latitude": coords["lat"],
            "longitude": coords["lon"],
            "daily": "sunshine_duration,wind_speed_10m_max",
            "timezone": "Europe/Paris",
            "forecast_days": 1,
        }
        
        response = requests.get(BASE_URL, params=params, timeout=15)
        response.raise_for_status()
        
        data = response.json()
        
        ensoleillement_h = data["daily"]["sunshine_duration"][0] / 3600.0
        vent_kmh = data["daily"]["wind_speed_10m_max"][0]
        
        resultats[region] = {
            "ensoleillement_h": ensoleillement_h,
            "vent_kmh": vent_kmh
        }
        
        logging.info(f"Région {region} traitée : {ensoleillement_h:.2f} h d'ensoleillement, vent max à {vent_kmh} km/h.")
        
    return resultats

def collecter_production_electrique(**context):
    """
    Collecte depuis éCO2mix la production solaire et éolienne par région.
    Agrège les valeurs horaires pour obtenir la moyenne journalière en MW.
    Retourne un dictionnaire {region: {solaire_mw: float, eolien_mw: float}}.
    """
    BASE_URL = (
        "https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets/eco2mix-regional-cons-def/records"
    )
    
    params = {
        "limit": 100, 
        "timezone": "Europe/Paris",
    }
    
    response = requests.get(BASE_URL, params=params, timeout=15)
    response.raise_for_status()
    records = response.json().get("results", [])

    accumulation = {region: {"solaire": [], "eolien": []} for region in REGIONS}

    for record in records:
        region = record.get("libelle_region")
        if region in REGIONS:
            solaire = record.get("solaire") or 0.0
            eolien = record.get("eolien") or 0.0
            
            accumulation[region]["solaire"].append(solaire)
            accumulation[region]["eolien"].append(eolien)

    production = {}
    for region, data in accumulation.items():
        nb_solaire = len(data["solaire"])
        nb_eolien = len(data["eolien"])
        
        moyenne_solaire = sum(data["solaire"]) / nb_solaire if nb_solaire > 0 else 0.0
        moyenne_eolien = sum(data["eolien"]) / nb_eolien if nb_eolien > 0 else 0.0
        
        production[region] = {
            "solaire_mw": round(moyenne_solaire, 2),
            "eolien_mw": round(moyenne_eolien, 2)
        }
        
        logging.info(f"Production agrégée {region} : {moyenne_solaire:.2f} MW Solaire, {moyenne_eolien:.2f} MW Éolien.")

    return production

def analyser_correlation(**context):
    """
    Corrèle les données météo et les données de production.
    Règles métier RTE :
    - Si ensoleillement > 6h ET production solaire <= 1000 MW → ALERTE solaire
    - Si vent > 30 km/h ET production éolienne <= 2000 MW → ALERTE éolien
    Retourne un dict d'alertes par région, sera stocké dans XCom.
    """
    ti = context["ti"]
    
    donnees_meteo = ti.xcom_pull(task_ids="collecter_meteo_regions") or {}
    donnees_production = ti.xcom_pull(task_ids="collecter_production_electrique") or {}
    
    alertes = {}
    
    for region in REGIONS:
        meteo = donnees_meteo.get(region, {})
        production = donnees_production.get(region, {})
        
        alertes_region = []
        
        ensoleillement = meteo.get("ensoleillement_h", 0)
        vent = meteo.get("vent_kmh", 0)
        solaire = production.get("solaire_mw", 0)
        eolien = production.get("eolien_mw", 0)
        
        if ensoleillement > 6 and solaire <= 1000:
            alertes_region.append(f"ALERTE SOLAIRE : {ensoleillement:.1f}h de soleil mais seulement {solaire:.0f} MW produits")
            
        if vent > 30 and eolien <= 2000:
            alertes_region.append(f"ALERTE ÉOLIEN : vent à {vent:.1f} km/h mais seulement {eolien:.0f} MW produits")
            
        if solaire > 0 and ensoleillement == 0:
            alertes_region.append(f"ANOMALIE DONNÉES : {solaire:.0f} MW solaires produits sans ensoleillement mesuré")
            
        alertes[region] = {
            "alertes": alertes_region,
            "ensoleillement_h": ensoleillement,
            "vent_kmh": vent,
            "solaire_mw": solaire,
            "eolien_mw": eolien,
            "statut": "ALERTE" if alertes_region else "OK",
        }
        
    nb_alertes = sum(1 for r in alertes.values() if r["statut"] == "ALERTE")
    logging.warning(f"{nb_alertes} région(s) en alerte sur {len(REGIONS)} analysées.")
    
    return alertes

def generer_rapport_energie(**context):
    """
    Génère un rapport JSON et affiche un tableau comparatif dans les logs Airflow.
    Sauvegarde le rapport dans /tmp/rapport_energie_.json.
    Retourne le chemin du fichier généré (stocké dans XCom).
    """
    ti = context["ti"]
    analyse = ti.xcom_pull(task_ids="analyser_correlation") or {}
    
    # Remplacement de date.today() par context["ds"]
    logical_date = context["ds"]

    # --- Affichage tableau dans les logs Airflow ---
    print("\n" + "=" * 80)
    print(f" RAPPORT ENERGIE & METEO — RTE — {logical_date}")
    print("=" * 80)
    print(
        f"{'Region':<25} {'Soleil (h)':>10} {'Vent (km/h)':>12} "
        f"{'Solaire (MW)':>13} {'Eolien (MW)':>12} {'Statut':>8}"
    )
    print("-" * 80)
    
    for region, data in analyse.items():
        print(
            f"{region:<25} {data.get('ensoleillement_h', 0):>10.1f} {data.get('vent_kmh', 0):>12.1f} "
            f"{data.get('solaire_mw', 0):>13.0f} {data.get('eolien_mw', 0):>12.0f} {data.get('statut', 'N/A'):>8}"
        )
        
    print("=" * 80 + "\n")

    # --- Construction du rapport JSON ---
    rapport = {
        "date": logical_date,
        "source": "RTE eCO2mix + Open-Meteo",
        "pipeline": "energie_meteo_dag",
        "regions": analyse,
        "resume": {
            "nb_regions_analysees": len(analyse),
            "nb_alertes": sum(1 for r in analyse.values() if r.get("statut") == "ALERTE"),
            "regions_en_alerte": [
                r for r, d in analyse.items() if d.get("statut") == "ALERTE"
            ],
        },
    }
    
    chemin = f"/tmp/rapport_energie_{logical_date}.json"
    
    with open(chemin, "w", encoding="utf-8") as f:
        json.dump(rapport, f, ensure_ascii=False, indent=2)
        
    logging.info(f"Rapport sauvegardé : {chemin}")
    return chemin


# --- Définition du DAG ---
with DAG(
    dag_id="energie_meteo_dag",
    default_args=default_args,
    description="Corrélation météo / production énergétique — RTE",
    schedule="0 6 * * *",
    start_date=datetime(2024, 1, 1, tzinfo=local_tz),
    catchup=False,
    tags=["rte", "energie", "meteo", "open-data"],
) as dag:
    
    t1 = PythonOperator(
        task_id="verifier_apis",
        python_callable=verifier_apis,
    )

    t2 = PythonOperator(
        task_id="collecter_meteo_regions",
        python_callable=collecter_meteo_regions,
    )

    t3 = PythonOperator(
        task_id="collecter_production_electrique",
        python_callable=collecter_production_electrique,
    )

    t4 = PythonOperator(
        task_id="analyser_correlation",
        python_callable=analyser_correlation,
    )

    t5 = PythonOperator(
        task_id="generer_rapport_energie",
        python_callable=generer_rapport_energie,
    )


    t1 >> [t2, t3] >> t4 >> t5