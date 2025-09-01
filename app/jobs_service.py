# app/jobs_service.py
from __future__ import annotations

import os
from typing import Optional

from decouple import config
from fastapi import HTTPException

from app.domain import repository as repo
from app.services.ftp_client import ftp_download
from app.database import SessionLocal

# Parsers/ETL disponibles
from app.services.telcel_v1 import run_telcel_v1_etl
from app.services.movistar import run_movistar_etl

# ===== Config =====
FTP_HOST      = config("FTP_HOST", default="ftp://192.168.100.200/")
FTP_USER_RO   = config("FTP_USER_RO", default="")
FTP_PASS_RO   = config("FTP_PASS_RO", default="")
LOCAL_TMP_DIR = config("LOCAL_TMP_DIR", default="/tmp/sabanas")  # en Windows ajústalo en .env (p.ej. C:/tmp/sabanas)


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _detect_provider_from_row(row: dict) -> str:
    """
    Intenta inferir la compañía/proveedor a partir del registro en DB.
    Devuelve uno de: 'TELCEL' | 'MOVISTAR'
    Default: 'TELCEL' (para mantener compatibilidad hacia atrás).
    """
    # Intentar varias claves posibles
    candidates = []
    for k in ("compania", "compañia", "carrier", "operador", "proveedor", "company", "company_name"):
        if k in row and row[k]:
            candidates.append(str(row[k]).strip().upper())

    # A veces existe un id numérico; si lo tienes normaliza aquí:
    # id_compania_telefonica: 1=Telcel, 2=Movistar, etc. (ajústalo si aplica)
    if "id_compania_telefonica" in row and row["id_compania_telefonica"] is not None:
        try:
            cid = int(row["id_compania_telefonica"])
            if cid == 2:
                return "MOVISTAR"
            if cid == 1:
                return "TELCEL"
        except Exception:
            pass

    for val in candidates:
        if "MOVISTAR" in val or "TELEFONICA" in val or "TELEFÓNICA" in val:
            return "MOVISTAR"
        if "TELCEL" in val:
            return "TELCEL"

    # Por defecto, asumimos Telcel
    return "TELCEL"


def _normalize_inserted_from_result(result) -> int:
    """
    Cada ETL puede devolver distintos formatos:
    - Telcel v1: int (insertadas) o -1 en error
    - Movistar: dict con 'total_insertadas' o 'total_normalizadas'
    Esta función estandariza a un int de filas insertadas.
    """
    if isinstance(result, int):
        return result

    if isinstance(result, dict):
        if "total_insertadas" in result and result["total_insertadas"] is not None:
            try:
                return int(result["total_insertadas"])
            except Exception:
                pass
        if "total_normalizadas" in result and result["total_normalizadas"] is not None:
            try:
                return int(result["total_normalizadas"])
            except Exception:
                pass

    # Si no se puede inferir, tratamos como 0 (no insertó filas)
    return 0


# -----------------------------------------------------------------------------
# API de Jobs
# -----------------------------------------------------------------------------
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
    mark_processed_after_download: bool = False,  # mantenido por compatibilidad
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

        # ===== ETL =====
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


def run_etl(id_archivo: int, local_path: str, correlation_id: Optional[str] = None) -> bool:
    """
    Despacha al ETL correcto según la compañía del archivo.
    Estandariza el resultado a True/False.
    """
    # Traer la fila para detectar proveedor
    db = SessionLocal()
    try:
        row = repo.get_archivo_by_id(db, id_archivo)
        if not row:
            print(f"[{correlation_id}] No existe id_archivo={id_archivo}")
            return False

        provider = _detect_provider_from_row(row)
        print(f"[{correlation_id}] Ejecutando ETL para proveedor={provider} id={id_archivo}")

        if provider == "MOVISTAR":
            # Movistar necesita la sesión de DB (su ETL inserta directamente)
            result = run_movistar_etl(db_session=db, id_sabanas=id_archivo, file_path=local_path)
            inserted = _normalize_inserted_from_result(result)

        else:
            # Telcel v1 (mantener firma original)
            result = run_telcel_v1_etl(
                id_sabanas=id_archivo,
                local_path=local_path,
                correlation_id=correlation_id
            )
            # Telcel v1 típicamente devuelve int (insertadas) o -1
            inserted = _normalize_inserted_from_result(result)

        if inserted == -1:
            print(f"[{correlation_id}] ETL devolvió error para id={id_archivo}")
            return False
        elif inserted == 0:
            print(f"[{correlation_id}] ETL no insertó filas (id={id_archivo})")
            # Decide si quieres tratar 0 como éxito o fallo; dejamos True como antes
            return True
        else:
            print(f"[{correlation_id}] ETL finalizó OK con {inserted} filas (id={id_archivo})")
            return True

    except Exception as ex:
        print(f"[{correlation_id}] Error en ETL (proveedor desconocido o fallo interno) id={id_archivo}: {ex}")
        return False
    finally:
        db.close()
