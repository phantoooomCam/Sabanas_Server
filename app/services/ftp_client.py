# app/services/ftp_client.py
import os
from ftplib import FTP
from typing import Tuple

def _normalize_host(ftp_host: str) -> str:
    return ftp_host.replace("ftp://", "").replace("ftps://", "").strip("/")

def ftp_download(ftp_host: str, username: str, password: str,
                 ruta_relativa: str, local_dir: str) -> str:
    """
    Descarga un archivo desde FTP a local_dir.
    ruta_relativa ej: 'ftp/upload/5512345678/archivo.xlsx'
    Devuelve la ruta local del archivo.
    """
    host = _normalize_host(ftp_host)
    ruta_relativa = ruta_relativa.lstrip("/")
    parts = ruta_relativa.split("/")
    *dirs, filename = parts

    os.makedirs(local_dir, exist_ok=True)
    local_path = os.path.join(local_dir, filename)

    with FTP(host) as ftp:
        ftp.login(user=username, passwd=password)
        # Navega por los directorios (si existen)
        for d in dirs:
            if not d:
                continue
            try:
                ftp.cwd(d)
            except Exception:
                # Si el primer segmento es 'ftp' porque guardaste 'ftp/upload/...'
                # intenta ignorarlo:
                pass
        with open(local_path, "wb") as f:
            ftp.retrbinary(f"RETR {filename}", f.write)

    return local_path
