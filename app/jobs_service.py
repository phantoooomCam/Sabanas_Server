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
from app.services.att import run_att_v1_etl
from app.services.altan import run_altan_v1_etl


PROVIDER_BY_ID = {
    # Telcel (todos sus “formatos”)
    1: "TELCEL",   # Telcel
    2: "TELCEL",   # TelcelNuevoFormato
    3: "TELCEL",   # TelcelIMEI
    14: "TELCEL",  # TelcelIMEINuevoFormato

    # AT&T (formatos)
    4: "ATT",      # AT&T
    13: "ATT",     # ATTNuevoFormato

    # Movistar
    5: "MOVISTAR", # Movistar

    # Altán
    12: "ALTAN",

    # Otros (si los quisieras enrutar distinto en el futuro)
    # 6: "VIRGIN",
    # 7: "BAIT",
    # 8: "TELMEX",
    # 9: "OXXO",
    # 10:"IZZI",
    # 11:"PERSONALIZADA",
}


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
    Devuelve 'TELCEL' | 'MOVISTAR' | 'ATT' | 'ALTAN'
    Default: 'TELCEL'
    """
    # 1) Por ID (lo más confiable)
    cid = row.get("id_compania_telefonica")
    if cid is not None:
        try:
            prov = PROVIDER_BY_ID.get(int(cid))
            if prov:
                return prov
        except Exception:
            pass

    # 2) Por nombre (strings en la fila)
    candidates = []
    for k in ("compania", "compañia", "carrier", "operador", "proveedor", "company", "company_name"):
        if k in row and row[k]:
            candidates.append(str(row[k]).strip().upper())

    for val in candidates:
        # Altán
        if "ALTAN" in val or "ALTÁN" in val:
            return "ALTAN"
        # Movistar
        if "MOVISTAR" in val or "TELEFONICA" in val or "TELEFÓNICA" in val:
            return "MOVISTAR"
        # Telcel (todas sus variantes)
        if "TELCEL" in val or "TELCELNUEVOFORMATO" in val or "TELCELIMEI" in val or "TELCELIMEINUEVOFORMATO" in val:
            return "TELCEL"
        # AT&T (todas sus variantes)
        if "AT&T" in val or "ATT" in val or "ATTMX" in val or "ATTNUEVOFORMATO" in val:
            return "ATT"

    # 3) Fallback por nombre de archivo (útil cuando DB viene vacío/inconsistente)
    try:
        fname = os.path.basename(row.get("file_path") or row.get("nombre_archivo") or "").upper()
        if "ALTAN" in fname or "ALTÁN" in fname:
            return "ALTAN"
        if "AT&T" in fname or "ATT" in fname:
            return "ATT"
        if "MOVISTAR" in fname or "TELEFONICA" in fname or "TELEFÓNICA" in fname:
            return "MOVISTAR"
        if "TELCEL" in fname:
            return "TELCEL"
    except Exception:
        pass

    # 4) Default
    return "TELCEL"


def _normalize_inserted_from_result(result) -> int:
    try:
        if isinstance(result, int):
            return result
        # si algún ETL devuelve dict/tuple, adapta aquí
        return int(result)
    except Exception:
        return -1


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
    Despacha al ETL correspondiente según la compañía.
    """
    db = SessionLocal()
    try:
        row = repo.get_archivo_by_id(db, id_archivo)
        if not row:
            print(f"[{correlation_id}] No existe id_archivo={id_archivo}")
            return False

        # Pista del camino: añade la ruta del archivo a row para que el detector pueda usarla
        row = dict(row)
        row.setdefault("file_path", local_path)

        provider = _detect_provider_from_row(row)

        # Fallback extra solo por el nombre del archivo recibido (por si acaso)
        try:
            fname = os.path.basename(local_path).upper()
            if "ALTAN" in fname or "ALTÁN" in fname:
                provider = "ALTAN"
            elif "AT&T" in fname or "ATT" in fname:
                provider = "ATT"
            elif "MOVISTAR" in fname or "TELEFONICA" in fname or "TELEFÓNICA" in fname:
                provider = "MOVISTAR"
            elif "TELCEL" in fname:
                provider = "TELCEL"
        except Exception:
            pass

        print(f"[{correlation_id}] Ejecutando ETL para proveedor={provider} id={id_archivo}")

        if provider == "MOVISTAR":
            result = run_movistar_etl(db_session=db, id_sabanas=id_archivo, file_path=local_path)
            inserted = _normalize_inserted_from_result(result)

        elif provider == "ATT":
            result = run_att_v1_etl(
                id_sabanas=id_archivo,
                local_path=local_path,
                correlation_id=correlation_id
            )
            inserted = _normalize_inserted_from_result(result)

        elif provider == "ALTAN":
            result = run_altan_v1_etl(
                db_session=db,
                id_sabanas=id_archivo,
                file_path=local_path
            )
            inserted = _normalize_inserted_from_result(result)

        else:  # TELCEL (default)
            result = run_telcel_v1_etl(
                id_sabanas=id_archivo,
                local_path=local_path,
                correlation_id=correlation_id
            )
            inserted = _normalize_inserted_from_result(result)

        if inserted == -1:
            print(f"[{correlation_id}] ETL devolvió error para id={id_archivo}")
            return False

        print(f"[{correlation_id}] ETL OK ({provider}) – filas insertadas: {inserted}")
        return True

    except Exception as ex:
        print(f"[{correlation_id}] Error en ETL id={id_archivo}: {ex}")
        return False
    finally:
        db.close()
