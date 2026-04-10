from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret
from kubernetes.client import models as k8s
from datetime import datetime

# Définition stricte de l'enveloppe de ressources
ressources_pod = k8s.V1ResourceRequirements(
    requests={"cpu": "100m", "memory": "128Mi"},
    limits={"cpu": "200m", "memory": "256Mi"},
)

# Injection sécurisée (Bonus 1)
# Prérequis d'infrastructure : Un secret Kubernetes nommé 'api-secrets' doit exister dans le namespace cible.
# Commande : kubectl create secret generic api-secrets --from-literal=token_secret=votre_valeur
secret_api = Secret(
    deploy_type='env',
    deploy_target='TOKEN_INJECTE',
    secret='api-secrets',
    key='token_secret'
)

with DAG(
    dag_id="exercice_kubernetes_pod_operator",
    start_date=datetime(2026, 4, 10),
    schedule=None,
    catchup=False,
    tags=["k8s", "isolation"]
) as dag:

    execution_pod = KubernetesPodOperator(
        task_id="tache_ephemere_k8s",
        name="worker-ephemere-python",
        namespace="default",
        image="python:3.9-slim",
        cmds=["python", "-c"],
        arguments=[
            "import os; "
            "print('Hello K8s'); "
            "print(f\"Validation Secret : {os.environ.get('TOKEN_INJECTE', 'Echec de montage')}\")"
        ],
        container_resources=ressources_pod,
        secrets=[secret_api],
        get_logs=True,                 
        is_delete_operator_pod=True,
        in_cluster=False, 
        config_file="/opt/airflow/.kube/config",
        cluster_context="docker-desktop" 
    )