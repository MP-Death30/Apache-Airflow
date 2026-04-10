import requests
import logging
from typing import Optional

logger = logging.getLogger(__name__)
WEBHDFS_BASE_URL = "http://hdfs-namenode:9870/webhdfs/v1"
WEBHDFS_USER = "root"

class WebHDFSClient:
    def __init__(self, base_url: str = WEBHDFS_BASE_URL, user: str = WEBHDFS_USER):
        self.base_url = base_url
        self.user = user

    def _url(self, path: str, op: str, **params) -> str:
        url = f"{self.base_url}{path}?op={op}&user.name={self.user}"
        for k, v in params.items():
            url += f"&{k}={v}"
        return url

    def mkdirs(self, hdfs_path: str) -> bool:
        url = self._url(hdfs_path, "MKDIRS")
        response = requests.put(url)
        response.raise_for_status()
        return response.json().get("boolean", False)

    def upload(self, hdfs_path: str, local_file_path: str) -> str:
        init_url = self._url(hdfs_path, "CREATE", overwrite="true")
        init_resp = requests.put(init_url, allow_redirects=False)
        
        if init_resp.status_code != 307:
            raise Exception(f"Echec initialisation upload HDFS: {init_resp.text}")
            
        datanode_url = init_resp.headers["Location"]
        with open(local_file_path, "rb") as f:
            upload_resp = requests.put(datanode_url, data=f, headers={"Content-Type": "application/octet-stream"})
            upload_resp.raise_for_status()
            
        return hdfs_path

    def open(self, hdfs_path: str) -> bytes:
        url = self._url(hdfs_path, "OPEN")
        response = requests.get(url, allow_redirects=True)
        response.raise_for_status()
        return response.content

    def exists(self, hdfs_path: str) -> bool:
        url = self._url(hdfs_path, "GETFILESTATUS")
        response = requests.get(url)
        return response.status_code == 200

    def list_status(self, hdfs_path: str) -> list:
        url = self._url(hdfs_path, "LISTSTATUS")
        response = requests.get(url)
        response.raise_for_status()
        return response.json().get("FileStatuses", {}).get("FileStatus", [])