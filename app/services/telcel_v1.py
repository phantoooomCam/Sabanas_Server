# app/services/telcel_v1.py
from __future__ import annotations

import os
import re
import unicodedata
from typing import Optional, List, Dict
from enum import Enum

import pandas as pd

from app.database import SessionLocal
from app.domain import repository as repo


# -------------------------------------------------------------------
# Enum TipoRegistroSabana (IDs iguales al enum/tablas)
# -------------------------------------------------------------------
class TipoRegistroSabana(Enum):
    DATOS = 0
    MMS = 1
    SMS_2VIAS_ENT = 2
    SMS_2VIAS_SAL = 3
    VOZ_ENTRANTE = 4
    VOZ_SALIENTE = 5
    VOZ_TRANSFER = 6
    VOZ_TRANSITO = 7
    NINGUNO = 8
    WIFI = 9
    REENVIO_SAL = 10
    REENVIO_ENT = 11


# -------------------------------------------------------------------
# Utilidades
# -------------------------------------------------------------------
EXPECTED_HEADER_TOKENS = {
    "telefono", "tipo", "numero a", "numero b", "fecha", "hora",
    "durac", "imei", "latitud", "longitud", "azimuth"
}

DMS_RE = re.compile(
    r"^\s*(\d+)[°\s]+(\d+)[\'’\s]+([\d\.]+)[\"\s]*([NSEWnsewo])?\s*$"
)

def _norm(s: str) -> str:
    if s is None:
        return ""
    s = str(s)
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower().strip()
    s = s.replace(".", " ").replace("_", " ")
    s = re.sub(r"\s+", " ", s)
    return s

def _score_header_row(values) -> int:
    toks = {_norm(v) for v in values if v is not None and str(v).strip() != ""}
    hits = 0
    for e in EXPECTED_HEADER_TOKENS:
        if any(e in t for t in toks):
            hits += 1
    return hits

def es_numero_valido(num: Optional[str]) -> bool:
    """Solo valida si parece un MSISDN; si no lo es, igual lo guardaremos crudo."""
    if num is None:
        return False
    s = str(num).strip().lower()
    if s == "":
        return False
    if s in ("internet.itelcel.com", "ims"):
        return False
    if s.startswith("telcel"):
        return False
    return True

def _clean_msisdn(x: str) -> Optional[str]:
    """Devuelve solo dígitos si luce como número; si no, None (luego dejamos el crudo)."""
    if not es_numero_valido(x):
        return None
    only = re.sub(r"\D", "", str(x))
    return only if only else None

def _clean_imei(x: str) -> Optional[str]:
    if x is None or str(x).strip() == "":
        return None
    s = str(x)
    s = re.sub(r"\D", "", s)
    if not s:
        return None
    return s[:15]

def _dms_to_decimal(s: str) -> Optional[float]:
    if s is None:
        return None
    s = str(s).strip().replace(",", ".")
    # Ya vienen en decimal
    if "°" not in s and "'" not in s and '"' not in s:
        try:
            return float(s)
        except Exception:
            return None
    m = DMS_RE.match(s)
    if not m:
        return None
    deg, minu, sec, hemi = m.groups()
    val = float(deg) + float(minu) / 60.0 + float(sec) / 3600.0
    if hemi and hemi.upper() in ("S", "W", "O"):
        val = -val
    return val


# -------------------------------------------------------------------
# Mapeo de tipo (tolerante a variantes comunes)
# -------------------------------------------------------------------
def _map_tipo(raw: str, numero_a: str = None, telefono: str = None) -> int:
    t = _norm(raw or "")
    if t.startswith("datos"):
        return TipoRegistroSabana.DATOS.value
    if t.startswith("mms") or "multimedia" in t:
        return TipoRegistroSabana.MMS.value
    if t.startswith("mensaj") and ("ent" in t or "entr" in t):
        return TipoRegistroSabana.SMS_2VIAS_ENT.value
    if t.startswith("mensaj") and ("sal" in t or "saliente" in t):
        return TipoRegistroSabana.SMS_2VIAS_SAL.value
    if t.startswith("voz entrante") or (t.startswith("voz") and "entr" in t):
        return TipoRegistroSabana.VOZ_ENTRANTE.value
    if t.startswith("voz saliente") or (t.startswith("voz") and "sal" in t):
        return TipoRegistroSabana.VOZ_SALIENTE.value
    if "transfer" in t:
        return TipoRegistroSabana.VOZ_TRANSFER.value
    if "transito" in t or "tránsito" in t:
        return TipoRegistroSabana.VOZ_TRANSITO.value
    if "wifi" in t:
        return TipoRegistroSabana.WIFI.value
    if "reenv" in t and ("sal" in t or "saliente" in t):
        return TipoRegistroSabana.REENVIO_SAL.value
    if "reenv" in t and ("ent" in t or "entrante" in t):
        return TipoRegistroSabana.REENVIO_ENT.value
    return TipoRegistroSabana.NINGUNO.value


# -------------------------------------------------------------------
# Detección de tabla y normalización
# -------------------------------------------------------------------
CANON_COLS_MAP = {
    "telefono": "telefono",
    "tipo": "tipo",
    "numero a": "numero_a",
    "numero b": "numero_b",
    "fecha": "fecha",
    "hora": "hora",
    "durac": "durac_seg",
    "imei": "imei",
    "latitud": "latitud",
    "longitud": "longitud",
    "azimuth": "azimuth",
}

def _canon_name(raw: str) -> str:
    n = _norm(raw)
    for key, target in CANON_COLS_MAP.items():
        if n.startswith(key):
            return target
    return n.replace(" ", "_")

def _find_table_in_sheet(df_raw: pd.DataFrame) -> Optional[pd.DataFrame]:
    max_scan = min(600, len(df_raw))
    best_idx, best_score = None, -1
    for i in range(max_scan):
        score = _score_header_row(df_raw.iloc[i, :].tolist())
        if score > best_score:
            best_score = score
            best_idx = i
        if score >= 6:
            break
    if best_idx is None or best_score < 5:
        return None
    raw_headers = df_raw.iloc[best_idx, :].tolist()
    canon_headers = [_canon_name(h) for h in raw_headers]
    df = df_raw.iloc[best_idx + 1 :].copy()
    df.columns = canon_headers
    df = df.dropna(axis=1, how="all")
    # strip de strings
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    return df

def _load_all_sheets(local_path: str) -> List[pd.DataFrame]:
    ext = os.path.splitext(local_path)[1].lower()
    frames: List[pd.DataFrame] = []
    if ext in (".xlsx", ".xls"):
        book = pd.read_excel(local_path, header=None, dtype=str, sheet_name=None, engine="openpyxl")
        for _, df_raw in book.items():
            tbl = _find_table_in_sheet(df_raw)
            if tbl is not None and len(tbl) > 0:
                frames.append(tbl)
    elif ext in (".csv", ".txt"):
        df_raw = pd.read_csv(local_path, header=None, dtype=str)
        tbl = _find_table_in_sheet(df_raw)
        if tbl is not None and len(tbl) > 0:
            frames.append(tbl)
    else:
        raise ValueError(f"Formato no soportado: {ext}")
    return frames


# -------------------------------------------------------------------
# Parseo robusto de fecha/hora (intenta varios formatos comunes Telcel)
# -------------------------------------------------------------------
_FORMATOS_DATETIME = [
    "%d/%m/%Y %H:%M:%S",
    "%d/%m/%y %H:%M:%S",
    "%d-%m-%Y %H:%M:%S",
    "%d-%m-%y %H:%M:%S",
]

def _parse_fecha_hora(fecha: pd.Series, hora: pd.Series) -> pd.Series:
    f = fecha.astype(str).str.strip().str.replace(r"[./]", "-", regex=True)
    h = hora.astype(str).str.strip()
    combo = f + " " + h
    ts = pd.Series([pd.NaT] * len(combo))
    for fmt in _FORMATOS_DATETIME:
        cand = pd.to_datetime(combo, format=fmt, errors="coerce")
        # si mejoró, nos quedamos
        if cand.notna().sum() > ts.notna().sum():
            ts = cand
        if ts.notna().all():
            break
    if ts.isna().all():
        ts = pd.to_datetime(combo, dayfirst=True, errors="coerce")
    return ts


# -------------------------------------------------------------------
# Normalización de filas
# -------------------------------------------------------------------
def _frame_to_rows(tbl: pd.DataFrame, id_sabanas: int) -> List[Dict]:
    cols = set(map(str, tbl.columns))

    if "fecha" in cols and "hora" in cols:
        fecha_hora = _parse_fecha_hora(tbl["fecha"], tbl["hora"])
    elif "fecha" in cols:
        # Intenta solo fecha
        f = tbl["fecha"].astype(str).str.strip().str.replace(r"[./]", "-", regex=True)
        fecha_hora = pd.to_datetime(f, dayfirst=True, errors="coerce")
    else:
        fecha_hora = pd.Series([pd.NaT] * len(tbl))

    durac = pd.to_numeric(tbl.get("durac_seg"), errors="coerce")
    numero_a = tbl.get("numero_a")
    numero_b = tbl.get("numero_b")
    lat_raw = tbl.get("latitud")
    lon_raw = tbl.get("longitud")
    azimuth_raw = pd.to_numeric(tbl.get("azimuth"), errors="coerce")
    imei_raw = tbl.get("imei")
    tel_raw = tbl.get("telefono")
    tipo_raw = tbl.get("tipo")

    rows: List[Dict] = []
    for i in range(len(tbl)):
        fa = fecha_hora.iloc[i]
        dur = durac.iloc[i] if not pd.isna(durac.iloc[i]) else None

        raw_a = numero_a.iloc[i] if numero_a is not None else None
        raw_b = numero_b.iloc[i] if numero_b is not None else None

        # 1) Si es MSISDN, dejar solo dígitos; si no, guardar el texto original
        a_clean = _clean_msisdn(raw_a) if raw_a is not None else None
        b_clean = _clean_msisdn(raw_b) if raw_b is not None else None
        a = a_clean if a_clean is not None else (str(raw_a).strip() if raw_a not in (None, "") else None)
        b = b_clean if b_clean is not None else (str(raw_b).strip() if raw_b not in (None, "") else None)

        lat_val = lat_raw.iloc[i] if lat_raw is not None else None
        lon_val = lon_raw.iloc[i] if lon_raw is not None else None
        lat_d = _dms_to_decimal(lat_val) if lat_val not in (None, "", "NaN") else None
        lon_d = _dms_to_decimal(lon_val) if lon_val not in (None, "", "NaN") else None
        az = azimuth_raw.iloc[i] if azimuth_raw is not None and not pd.isna(azimuth_raw.iloc[i]) else None
        imei = _clean_imei(imei_raw.iloc[i]) if imei_raw is not None else None

        # 2) Telefono: mismo criterio — si no es dígitos, guardar crudo
        raw_tel = tel_raw.iloc[i] if tel_raw is not None else None
        tel_clean = _clean_msisdn(raw_tel) if raw_tel is not None else None
        tel = tel_clean if tel_clean is not None else (
            str(raw_tel).strip() if raw_tel not in (None, "") else (a or None)
        )

        tipo_val = tipo_raw.iloc[i] if tipo_raw is not None else None
        id_tipo = _map_tipo(tipo_val, numero_a=a, telefono=tel)

        # 3) Altitud siempre 0; coordenada_objetivo = False solo si NO hay coords
        coord_obj = False if (lat_d is None and lon_d is None) else None

        rows.append({
            "id_sabanas": int(id_sabanas),
            "numero_a": a,
            "numero_b": b,
            "id_tipo_registro": id_tipo,
            "fecha_hora": (fa.to_pydatetime() if isinstance(fa, pd.Timestamp) and not pd.isna(fa) else None),
            "duracion": int(dur) if dur is not None else None,
            "latitud": str(lat_val) if lat_val not in (None, "", "NaN") else None,
            "longitud": str(lon_val) if lon_val not in (None, "", "NaN") else None,
            "azimuth": float(az) if az is not None else None,
            "latitud_decimal": float(lat_d) if lat_d is not None else None,
            "longitud_decimal": float(lon_d) if lon_d is not None else None,
            "altitud": 0.0,                     # <--- siempre 0
            "coordenada_objetivo": coord_obj,   # <--- False si no hay coordenadas; en caso contrario NULL
            "imei": imei,
            "telefono": tel,
        })

    def _is_meaningful(r: Dict) -> bool:
        return any([
            r.get("numero_a"), r.get("numero_b"), r.get("fecha_hora"),
            r.get("duracion"), r.get("latitud_decimal"), r.get("longitud_decimal")
        ])

    return [r for r in rows if _is_meaningful(r)]


# -------------------------------------------------------------------
# API pública del parser
# -------------------------------------------------------------------
def run_telcel_v1_etl(id_sabanas: int, local_path: str, correlation_id: Optional[str] = None) -> int:
    frames = _load_all_sheets(local_path)
    if not frames:
        print(f"[{correlation_id}] No se detectó tabla Telcel en {local_path}")
        return 0

    all_rows: List[Dict] = []
    for tbl in frames:
        rows = _frame_to_rows(tbl, id_sabanas=id_sabanas)
        all_rows.extend(rows)

    # Deduplicación simple
    seen = set()
    deduped_registros = []
    for r in all_rows:
        key = (r.get("telefono"), r.get("numero_a"), r.get("numero_b"),
               r.get("fecha_hora"), r.get("imei"))
        if key not in seen:
            seen.add(key)
            deduped_registros.append(r)
    all_rows = deduped_registros

    imeis = {r["imei"]: r["imei"] for r in all_rows if r.get("imei")}
    unique_imeis = list(imeis.values())

    imsis = {r["numero_a"]: r["numero_a"] for r in all_rows if r.get("numero_a") and len(str(r["numero_a"])) > 12}
    unique_imsis = list(imsis.values())

    db = SessionLocal()
    try:
        repo.delete_registros_telefonicos_by_archivo(db, id_sabanas)
        if all_rows:
            repo.insert_registros_telefonicos_bulk(db, all_rows)
        print(f"[{correlation_id}] Telcel v1: insertadas {len(all_rows)} filas "
              f"(id_sabanas={id_sabanas}), {len(unique_imeis)} IMEIs únicos, {len(unique_imsis)} IMSIs únicos")
    finally:
        db.close()

    return len(all_rows)
