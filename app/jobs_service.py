# app/jobs_service.py
from __future__ import annotations

import os
from typing import Optional

from decouple import config
from fastapi import HTTPException

from app.domain import repository as repo
from app.services.ftp_client import ftp_download
from app.database import SessionLocal
from app.services.telcel_v1 import run_telcel_v1_etl

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

def run_etl(id_archivo: int, local_path: str, correlation_id: Optional[str] = None) -> bool:
    import os
    import re
    import unicodedata
    import pandas as pd
    from app.database import SessionLocal
    from app.domain import repository as repo

    # ===== Utilidades internas (solo para este ETL) =====
    EXPECTED_HEADER_TOKENS = {
        "telefono", "tipo", "numero a", "numero b", "fecha", "hora",
        "durac", "imei", "latitud", "longitud", "azimuth"
    }
    dms_re = re.compile(r"^\s*(\d+)[°\s]+(\d+)[\'’\s]+([\d\.]+)[\"\s]*([NSEWnsewo])?\s*$")

    def norm(s: str) -> str:
        if s is None:
            return ""
        s = str(s)
        s = unicodedata.normalize("NFKD", s)
        s = "".join(ch for ch in s if not unicodedata.combining(ch))
        s = s.lower().strip()
        s = s.replace(".", " ").replace("_", " ")
        s = re.sub(r"\s+", " ", s)
        return s

    def score_header_row(values) -> int:
        toks = {norm(v) for v in values if v is not None and str(v).strip() != ""}
        hits = 0
        for e in EXPECTED_HEADER_TOKENS:
            if any(e in t for t in toks):
                hits += 1
        return hits

    CANON_COLS_MAP = {
        "telefono": "telefono",
        "tipo": "tipo",
        "numero a": "numero_a",
        "numero b": "numero_b",
        "fecha": "fecha",
        "hora": "hora",
        "durac": "durac_seg",   # "durac", "durac seg", "durac. seg."
        "imei": "imei",
        "latitud": "latitud",
        "longitud": "longitud",
        "azimuth": "azimuth",
    }

    def canon_name(raw: str) -> str:
        n = norm(raw)
        for key, target in CANON_COLS_MAP.items():
            if n.startswith(key):
                return target
        return n.replace(" ", "_")

    def dms_to_decimal(s: str):
        if s is None:
            return None
        s = str(s).strip().replace(",", ".")
        # si ya parece decimal:
        if "°" not in s and "'" not in s and '"' not in s:
            try:
                return float(s)
            except Exception:
                return None
        m = dms_re.match(s)
        if not m:
            return None
        deg, minu, sec, hemi = m.groups()
        val = float(deg) + float(minu) / 60.0 + float(sec) / 3600.0
        if hemi and hemi.upper() in ("S", "W", "O"):
            val = -val
        return val

    def clean_msisdn(x: str):
        if x is None:
            return None
        x = str(x).strip().lower()
        if not x or any(bad in x for bad in ["ims", "internet"]):
            return None
        only = re.sub(r"\D", "", x)
        return only if only else None

    def clean_imei(x: str):
        if x is None or str(x).strip() == "":
            return None
        s = re.sub(r"\D", "", str(x))
        return s[:15] if s else None

    def map_tipo(raw: str) -> int:
        t = norm(raw or "")
        if "datos" in t:
            return 3
        if "sms" in t or "mensaje" in t:
            return 2
        return 1  # voz/llamada por defecto

    # ===== 0) Intento específico: Telcel con metadatos y tabla inferior =====
    inserted = 0
    try:
        ext = os.path.splitext(local_path)[1].lower()
        frames = []

        if ext in (".xlsx", ".xls"):
            book = pd.read_excel(local_path, header=None, dtype=str, sheet_name=None, engine="openpyxl")
            for _, df_raw in book.items():
                # Buscar fila de encabezado
                max_scan = min(600, len(df_raw))
                best_idx, best_score = None, -1
                for i in range(max_scan):
                    sc = score_header_row(df_raw.iloc[i, :].tolist())
                    if sc > best_score:
                        best_score, best_idx = sc, i
                    if sc >= 6:
                        break
                if best_idx is None or best_score < 5:
                    continue

                # Construir tabla con nombres canónicos
                raw_headers = df_raw.iloc[best_idx, :].tolist()
                canon_headers = [canon_name(h) for h in raw_headers]
                tbl = df_raw.iloc[best_idx + 1 :].copy()
                tbl.columns = canon_headers
                tbl = tbl.dropna(axis=1, how="all")
                for c in tbl.select_dtypes(include=["object"]).columns:
                    tbl[c] = tbl[c].map(lambda x: x.strip() if isinstance(x, str) else x)

                if len(tbl):
                    frames.append(tbl)

        elif ext in (".csv", ".txt"):
            df_raw = pd.read_csv(local_path, header=None, dtype=str)
            # Igual detección de encabezado en CSV
            max_scan = min(200, len(df_raw))
            best_idx, best_score = None, -1
            for i in range(max_scan):
                sc = score_header_row(df_raw.iloc[i, :].tolist())
                if sc > best_score:
                    best_score, best_idx = sc, i
                if sc >= 6:
                    break
            if best_idx is not None and best_score >= 5:
                raw_headers = df_raw.iloc[best_idx, :].tolist()
                canon_headers = [canon_name(h) for h in raw_headers]
                tbl = df_raw.iloc[best_idx + 1 :].copy()
                tbl.columns = canon_headers
                tbl = tbl.dropna(axis=1, how="all")
                tbl = tbl.applymap(lambda x: x.strip() if isinstance(x, str) else x)
                if len(tbl):
                    frames.append(tbl)

                # Si detectamos al menos una tabla, procesamos como Telcel
        if frames:
            rows = []
            for tbl in frames:
                # --- normaliza strings columna por columna (sin applymap deprecado)
                for c in tbl.select_dtypes(include=["object"]).columns:
                    tbl[c] = tbl[c].map(lambda x: x.strip() if isinstance(x, str) else x)

                cols = set(map(str, tbl.columns))

                # --- fecha/hora (evita 'serie or ""')
                if "fecha" in cols and "hora" in cols:
                    fecha_hora = pd.to_datetime(
                        tbl["fecha"].astype(str).str.strip() + " " + tbl["hora"].astype(str).str.strip(),
                        errors="coerce",
                        dayfirst=True,
                        format="%d/%m/%Y %H:%M:%S"  # <-- ajusta al formato más común en tus sábanas
                    )
                elif "fecha" in cols:
                    fecha_hora = pd.to_datetime(
                        tbl["fecha"].astype(str).fillna(""),
                        errors="coerce", dayfirst=True
                    )
                else:
                    fecha_hora = pd.Series([pd.NaT] * len(tbl))

                # --- duración
                durac = pd.to_numeric(tbl["durac_seg"], errors="coerce") if "durac_seg" in cols else pd.Series([pd.NA]*len(tbl))

                # --- columnas que pueden o no existir
                numero_a   = tbl["numero_a"] if "numero_a" in cols else None
                numero_b   = tbl["numero_b"] if "numero_b" in cols else None
                imei_raw   = tbl["imei"]     if "imei"     in cols else None
                tel_raw    = tbl["telefono"] if "telefono" in cols else None
                tipo_raw   = tbl["tipo"]     if "tipo"     in cols else None
                lat_raw    = tbl["latitud"]  if "latitud"  in cols else None
                lon_raw    = tbl["longitud"] if "longitud" in cols else None
                az_raw     = pd.to_numeric(tbl["azimuth"], errors="coerce") if "azimuth" in cols else None

                for i in range(len(tbl)):
                    # fecha y duración (escalares)
                    fa  = fecha_hora.iloc[i] if len(fecha_hora) > i else pd.NaT
                    dur = (int(durac.iloc[i]) if (len(durac) > i and pd.notna(durac.iloc[i])) else None)

                    # números (escalares)
                    a = clean_msisdn(numero_a.iloc[i]) if numero_a is not None else None
                    b = clean_msisdn(numero_b.iloc[i]) if numero_b is not None else None

                    # lat/lon (escalares)
                    lat_val = lat_raw.iloc[i] if lat_raw is not None else None
                    lon_val = lon_raw.iloc[i] if lon_raw is not None else None
                    lat_d = dms_to_decimal(lat_val) if (lat_val is not None and str(lat_val).strip() not in ("", "NaN")) else None
                    lon_d = dms_to_decimal(lon_val) if (lon_val is not None and str(lon_val).strip() not in ("", "NaN")) else None
                    az    = float(az_raw.iloc[i]) if (az_raw is not None and not pd.isna(az_raw.iloc[i])) else None

                    # imei / telefono (escalares)
                    imei = clean_imei(imei_raw.iloc[i]) if imei_raw is not None else None
                    tel  = clean_msisdn(tel_raw.iloc[i]) if tel_raw is not None else (a or None)

                    # tipo → id_tipo_registro (escalar)
                    tipo_val = tipo_raw.iloc[i] if tipo_raw is not None else None
                    id_tipo  = map_tipo(tipo_val)

                    # filtra filas totalmente vacías
                    tiene_fecha  = isinstance(fa, pd.Timestamp) and not pd.isna(fa)
                    if not any([a, b, tiene_fecha, dur, lat_d, lon_d]):
                        continue

                    rows.append({
                        "id_sabanas": int(id_archivo),
                        "numero_a": a,
                        "numero_b": b,
                        "id_tipo_registro": id_tipo,
                        "fecha_hora": (fa.to_pydatetime() if isinstance(fa, pd.Timestamp) and not pd.isna(fa) else None),
                        "duracion": dur,
                        "latitud": str(lat_val) if (lat_val is not None and str(lat_val).strip() not in ("", "NaN")) else None,
                        "longitud": str(lon_val) if (lon_val is not None and str(lon_val).strip() not in ("", "NaN")) else None,
                        "azimuth": az,
                        "latitud_decimal": lat_d,
                        "longitud_decimal": lon_d,
                        "altitud": None,
                        "coordenada_objetivo": None,
                        "imei": imei,
                        "telefono": tel,
                    })

            db = SessionLocal()
            try:
                repo.delete_registros_telefonicos_by_archivo(db, id_archivo)
                if rows:
                    repo.insert_registros_telefonicos_bulk(db, rows)
                inserted = len(rows)
                print(f"[{correlation_id}] Telcel: insertadas {inserted} filas (id_sabanas={id_archivo})")
            finally:
                db.close()

            return inserted > 0


    except Exception as e:
        print(f"[{correlation_id}] Error en parser Telcel: {e}")

    # ===== 1) Fallback genérico (tu lógica original) =====
    try:
        ext = os.path.splitext(local_path)[1].lower()
        if ext in [".xlsx", ".xls"]:
            df = pd.read_excel(local_path)
        elif ext in [".csv", ".txt"]:
            df = pd.read_csv(local_path)
        else:
            print(f"[{correlation_id}] Formato no soportado: {ext}")
            return False

        colmap = {
            "Fecha": "fecha_hora",
            "Hora": "hora",
            "NumeroA": "numero_a",
            "NumeroB": "numero_b",
            "Duracion": "duracion",
            "IMEI": "imei",
            "Telefono": "telefono",
            "Lat": "latitud",
            "Lon": "longitud",
            "Azimuth": "azimuth",
        }
        df = df.rename(columns={k: v for k, v in colmap.items() if k in df.columns})

        if "fecha_hora" not in df.columns and {"fecha", "hora"}.issubset({c.lower() for c in df.columns}):
            lcols = {c.lower(): c for c in df.columns}
            df["fecha_hora"] = pd.to_datetime(df[lcols["fecha"]] + " " + df[lcols["hora"]], errors="coerce")
        elif "fecha_hora" in df.columns:
            df["fecha_hora"] = pd.to_datetime(df["fecha_hora"], errors="coerce")

        tipo_map = {"LLAMADA": 1, "SMS": 2, "DATOS": 3}
        if "id_tipo_registro" not in df.columns:
            if "tipo" in df.columns:
                df["id_tipo_registro"] = df["tipo"].astype(str).str.upper().map(tipo_map).fillna(1).astype(int)
            else:
                df["id_tipo_registro"] = 1

        for c in ["latitud", "longitud", "latitud_decimal", "longitud_decimal", "azimuth", "altitud", "duracion"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")

        required = [
            "numero_a", "numero_b", "id_tipo_registro", "fecha_hora", "duracion",
            "latitud", "longitud", "azimuth", "latitud_decimal", "longitud_decimal",
            "altitud", "coordenada_objetivo", "imei", "telefono"
        ]
        for col in required:
            if col not in df.columns:
                df[col] = None

        df["numero_a"] = df["numero_a"].astype(str).str.replace(r"\D", "", regex=True)
        df["numero_b"] = df["numero_b"].astype(str).str.replace(r"\D", "", regex=True)

        rows = []
        for _, r in df.iterrows():
            rows.append({
                "id_sabanas": id_archivo,
                "numero_a": r["numero_a"] if pd.notna(r["numero_a"]) else None,
                "numero_b": r["numero_b"] if pd.notna(r["numero_b"]) else None,
                "id_tipo_registro": int(r["id_tipo_registro"]) if pd.notna(r["id_tipo_registro"]) else 1,
                "fecha_hora": r["fecha_hora"].to_pydatetime() if pd.notna(r["fecha_hora"]) else None,
                "duracion": int(r["duracion"]) if pd.notna(r["duracion"]) else None,
                "latitud": float(r["latitud"]) if pd.notna(r["latitud"]) else None,
                "longitud": float(r["longitud"]) if pd.notna(r["longitud"]) else None,
                "azimuth": float(r["azimuth"]) if pd.notna(r["azimuth"]) else None,
                "latitud_decimal": float(r["latitud_decimal"]) if pd.notna(r["latitud_decimal"]) else None,
                "longitud_decimal": float(r["longitud_decimal"]) if pd.notna(r["longitud_decimal"]) else None,
                "altitud": float(r["altitud"]) if pd.notna(r["altitud"]) else None,
                "coordenada_objetivo": r["coordenada_objetivo"] if pd.notna(r["coordenada_objetivo"]) else None,
                "imei": str(r["imei"]) if pd.notna(r["imei"]) else None,
                "telefono": str(r["telefono"]) if pd.notna(r["telefono"]) else None,
            })

        db = SessionLocal()
        try:
            repo.delete_registros_telefonicos_by_archivo(db, id_archivo)
            inserted = repo.insert_registros_telefonicos_bulk(db, rows)
            print(f"[{correlation_id}] Insertados {inserted} registros para id_sabanas={id_archivo}")
        finally:
            db.close()

        return True

    except Exception as e:
        print(f"[{correlation_id}] Error en ETL genérico: {e}")
        return False
