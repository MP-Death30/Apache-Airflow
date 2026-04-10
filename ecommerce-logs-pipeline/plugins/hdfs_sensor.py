from airflow.sensors.base import BaseSensorOperator
import requests

class HdfsFileSensor(BaseSensorOperator):
    """Sensor qui attend qu'un fichier existe dans HDFS."""
    
    # Activation stricte du templating Jinja
    template_fields = ("hdfs_path", "namenode_url")
    
    def __init__(self, hdfs_path: str, namenode_url: str, **kwargs):
        super().__init__(**kwargs)
        self.hdfs_path = hdfs_path
        self.namenode_url = namenode_url

    def poke(self, context) -> bool:
        try:
            resp = requests.get(
                f"{self.namenode_url}/webhdfs/v1{self.hdfs_path}",
                params={"op": "GETFILESTATUS", "user.name": "root"},
                timeout=5,
            )
            
            if resp.status_code == 200:
                size = resp.json()["FileStatus"]["length"]
                self.log.info("Fichier trouvé : %s (%d bytes)", self.hdfs_path, size)
                return True
            
            # Traçabilité des échecs (ex: 404 Not Found)
            self.log.info("En attente. Statut HTTP: %s | Fichier ciblé: %s", resp.status_code, self.hdfs_path)
            return False
            
        except Exception as exc:
            self.log.warning("Erreur réseau ou indisponibilité HDFS : %s", exc)
            return False