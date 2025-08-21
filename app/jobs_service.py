# app/jobs_service.py
from __future__ import annotations

import os
from typing import Optional

from decouple import config
from fastapi import HTTPException

from app.domain import repository as repo
from app.services.ftp_client import ftp_download
from app.database import SessionLocal

# ===== Config =====
FTP_HOST     = config("FTP_HOST", default="ftp://192.168.100.200/")
FTP_USER_RO  = config("FTP_USER_RO", default="")
FTP_PASS_RO  = config("FTP_PASS_RO", default="")
LOCAL_TMP_DIR = config("LOCAL_TMP_DIR", default="/tmp/sabanas")  # en Windows cámbialo en .env ej. C:/tmp/sabanas


def accept_job_sabana(id_archivo: int) -> tuple[str, dict]:
    """
    subido -> en_cola (transición atómica).
    Devuelve (job_id, row_dict).
    """
    import uuid

    db = SessionLocal()
    try:
        row = repo.get_archivo_by_id(db, id_archivo)
        if not row:
            raise HTTPException(status_code=404, detail="Archivo no encontrado")

        if row["estado"] != "subido":
            # Si ya está en en_cola/procesando/procesado/error devolvemos 409
            raise HTTPException(status_code=409, detail=f"Estado actual: {row['estado']}")

        ok = repo.try_mark_estado(db, id_archivo, expected="subido", new_state="en_cola")
        if not ok:
            # Otro worker lo tomó o cambió de estado
            raise HTTPException(status_code=409, detail="No se pudo reservar el job (estado cambió)")

        return str(uuid.uuid4()), row
    finally:
        db.close()


def process_job_sabana(
    id_archivo: int,
    correlation_id: Optional[str] = None,
    mark_processed_after_download: bool = False,  # ya no lo usaremos; dejamos por compatibilidad
) -> None:
    """
    Worker:
      en_cola -> procesando (setea fecha_inicio)
      descarga FTP -> run_etl(...)
      si OK: procesado (setea fecha_termino)
      si falla: error (setea fecha_termino)
    """
    db = SessionLocal()
    try:
        row = repo.get_archivo_by_id(db, id_archivo)
        if not row or row["estado"] != "en_cola":
            # Nada que hacer o ya lo tomó otro
            return

        # en_cola -> procesando
        if not repo.try_mark_estado(
            db, id_archivo, expected="en_cola", new_state="procesando", set_inicio=True
        ):
            return  # otro worker lo movió

        # ===== Descarga desde FTP =====
        ruta_relativa = row["ruta"]  # ej: ftp/upload/5512345678/archivo.xlsx
        local_dir = os.path.join(LOCAL_TMP_DIR, str(id_archivo))
        try:
            os.makedirs(local_dir, exist_ok=True)
            local_path = ftp_download(
                FTP_HOST, FTP_USER_RO, FTP_PASS_RO, ruta_relativa, local_dir
            )
        except Exception as e:
            # Marca error y registra log
            repo.mark_error(db, id_archivo)
            print(f"[{correlation_id}] Error descargando id={id_archivo}: {e}")
            return

        # ===== ETL (stub por ahora) =====
        try:
            ok = run_etl(id_archivo=id_archivo, local_path=local_path, correlation_id=correlation_id)
            if not ok:
                repo.mark_error(db, id_archivo)
                print(f"[{correlation_id}] ETL devolvió False para id={id_archivo}")
                return
        except Exception as e:
            repo.mark_error(db, id_archivo)
            print(f"[{correlation_id}] Error en ETL id={id_archivo}: {e}")
            return

        # ===== Cerrar con PROCESADO =====
        repo.try_mark_estado(
            db, id_archivo, expected="procesando", new_state="procesado", set_termino=True
        )

    finally:
        db.close()


# ===========================
# Stub del ETL (mínimo)
# ===========================
def run_etl(
    id_archivo: int,
    local_path: str,
    correlation_id: Optional[str] = None,
) -> bool:
    """
    ETL MÍNIMO (stub):
      - Valida que el archivo descargado exista y sea accesible.
      - Aquí, en la siguiente fase, leerás el XLSX/CSV, normalizarás e insertarás
        en sabanas.registros (u otras tablas).
      - Devuelve True si todo bien; False/lanza excepción si algo falla.
    """
    # Validación mínima: archivo existe y tiene tamaño > 0
    if not os.path.exists(local_path):
        print(f"[{correlation_id}] Archivo no existe: {local_path}")
        return False
    if os.path.getsize(local_path) <= 0:
        print(f"[{correlation_id}] Archivo vacío: {local_path}")
        return False

    # TODO: implementar lectura + normalización + insert en DB en la siguiente etapa.
    # Por ahora lo consideramos OK si pasa las validaciones previas.
    print(f"[{correlation_id}] ETL OK (stub) id={id_archivo} file={local_path}")
    return True
