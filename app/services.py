# app/services.py
import uuid
import os
from fastapi import HTTPException, status
from app.domain import repository as repo
from app.services.ftp_client import ftp_download
from app.database import SessionLocal  # ajusta según tu database.py

FTP_HOST = os.getenv("FTP_HOST", "ftp://192.168.100.200/")
FTP_USER_RO = os.getenv("FTP_USER_RO", "")
FTP_PASS_RO = os.getenv("FTP_PASS_RO", "")

def accept_job_sabana(id_archivo: int) -> tuple[str, dict]:
    """
    Marca subido -> en_cola. Devuelve (job_id, row_dict).
    """
    db = SessionLocal()
    try:
        row = repo.get_archivo_by_id(db, id_archivo)
        if not row:
            raise HTTPException(status_code=404, detail="Archivo no encontrado")

        if row["estado"] != "subido":
            raise HTTPException(status_code=409, detail=f"Estado actual: {row['estado']}")

        ok = repo.try_mark_estado(db, id_archivo, expected="subido", new_state="en_cola")
        if not ok:
            # Otro worker lo tomó
            raise HTTPException(status_code=409, detail="No se pudo reservar el job (estado cambió)")

        return str(uuid.uuid4()), row
    finally:
        db.close()

def process_job_sabana(id_archivo: int, correlation_id: str | None = None, mark_processed_after_download: bool = False):
    """
    Worker simple: en_cola -> procesando, descarga FTP y (opcional) deja procesado.
    """
    db = SessionLocal()
    try:
        row = repo.get_archivo_by_id(db, id_archivo)
        if not row or row["estado"] != "en_cola":
            return  # nada que hacer

        # en_cola -> procesando
        if not repo.try_mark_estado(db, id_archivo, expected="en_cola", new_state="procesando", set_inicio=True):
            return

        # descarga
        ruta_relativa = row["ruta"]  # ej: ftp/upload/5512345678/archivo.xlsx
        local_dir = f"/tmp/sabanas/{id_archivo}"
        try:
            local_path = ftp_download(FTP_HOST, FTP_USER_RO, FTP_PASS_RO, ruta_relativa, local_dir)
            # Para la prueba: si quieres, cierra el ciclo marcando procesado al terminar la descarga:
            if mark_processed_after_download:
                repo.try_mark_estado(db, id_archivo, expected="procesando", new_state="procesado", set_termino=True)
        except Exception as e:
            # Marca error
            repo.mark_error(db, id_archivo)
            # Loguea; evita imprimir credenciales
            print(f"[{correlation_id}] Error descargando id={id_archivo}: {e}")
    finally:
        db.close()
