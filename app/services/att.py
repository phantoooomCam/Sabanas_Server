# app/services/att_v1.py
from __future__ import annotations

import os
import re
import unicodedata
from typing import Optional, List, Dict, Tuple
from enum import Enum
import math
import pandas as pd

from app.database import SessionLocal
from app.domain import repository as repo


# -------------------------------------------------------------------
# Enum TipoRegistroSabana (IDs deben coincidir con tu BD)
# -------------------------------------------------------------------
class TipoRegistroSabana(Enum):
    Datos = 0
    MensajeriaMultimedia = 1
    Mensaje2ViasEnt = 2
    Mensaje2ViasSal = 3
    VozEntrante = 4
    VozSaliente = 5
    VozTransfer = 6
    VozTransito = 7
    Ninguno = 8
    Wifi = 9
    ReenvioSal = 10
    ReenvioEnt = 11


# -------------------------------------------------------------------
# Utilidades
# -------------------------------------------------------------------
EXPECTED_HEADER_TOKENS = {
    # comunes v1
    "telefono", "teléfono", "tipo",
    "numero a", "número a", "num a",
    "numero b", "número b", "num b", "dest",
    "fecha", "hora",
    "durac", "durac seg", "durac. seg.", "duración", "dur",  # <- AT&T usa DUR
    "imei", "num a imei",
    "latitud", "longitud", "azimuth",
    # AT&T
    "serv", "t_reg",
    # futuros (comentados por ahora)
    "id celda", "num a imsi", "pais", "causa t", "tipo com",
}

AZ_LIST_RE = re.compile(r"^\[\s*(.*?)\s*\]$")

def _parse_azimuth(val) -> Optional[float]:
    """Devuelve el primer valor numérico. Si viene como lista '[a:b:c]' toma 'a'."""
    if val is None:
        return None
    s = str(val).strip().replace(",", ".")
    if s == "" or s.lower() == "nan":
        return None
    m = AZ_LIST_RE.match(s)
    if m:
        for part in m.group(1).split(":"):
            part = part.strip()
            if not part:
                continue
            try:
                return float(part)
            except Exception:
                continue
        return None
    try:
        return float(s)
    except Exception:
        return None


DMS_RE = re.compile(r"^\s*(\d+)[°\s]+(\d+)[\'’\s]+([\d\.]+)\s*([NSEWnsewo])?[\"\s]*$")

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
    if num is None:
        return False
    s = str(num).strip().lower()
    return s != ""

def _clean_msisdn(x: str) -> Optional[str]:
    if not es_numero_valido(x):
        return None
    s = re.sub(r"\D", "", str(x))
    if not s:
        return None
    # quitar prefijos de país "52" (si sobra más de 10 dígitos)
    while s.startswith("52") and len(s) > 10:
        s = s[2:]
    return s

def _clean_imei(x: str) -> Optional[str]:
    if x is None or str(x).strip() == "":
        return None
    s = re.sub(r"\D", "", str(x))
    if not s:
        return None
    return s[:15]

def _dms_to_decimal(s: str) -> Optional[float]:
    if s is None:
        return None
    # Convertir a string y verificar valores inválidos
    s_str = str(s).strip().lower()
    if s_str in ("", "nan", "none", "null", "na", "n/a"):
        return None
    s = s_str.replace(",", ".").replace(""", '"').replace(""", '"')
    if "°" not in s and "'" not in s and '"' not in s:
        try:
            val = float(s)
            # Verificar si es NaN
            if math.isnan(val):
                return None
            return val
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

def _extract_msisdn_from_filename(path: str) -> Optional[str]:
    name = os.path.basename(path)
    digits = re.findall(r"\d{8,}", name)
    if not digits:
        return None
    digits.sort(key=len, reverse=True)
    msisdn = digits[0]
    # aplicar misma lógica de limpieza
    while msisdn.startswith("52") and len(msisdn) > 10:
        msisdn = msisdn[2:]
    return msisdn

def _pick_last_nonzero(value: Optional[str]) -> Optional[str]:
    """
    Acepta:
      - '19.4302'     -> '19.4302'
      - '[19.43:0:19.45]' -> '19.45' (último no-cero)
      - '[0:0:0]'     -> '0'
    """
    if value is None:
        return None
    s = str(value).strip()
    if s == "":
        return None
    # detectar lista [a:b:c]
    m = re.match(r"^\[\s*(.*?)\s*\]$", s)
    if not m:
        return s
    parts = [p.strip() for p in m.group(1).split(":")]
    # elegir último no-cero si es posible
    chosen = None
    for p in reversed(parts):
        try:
            if float(str(p).replace(",", ".")) != 0.0:
                chosen = p
                break
        except Exception:
            # si no es numérico, lo podemos tomar como candidato textual
            if p not in ("",):
                chosen = p
                break
    if chosen is None:
        chosen = parts[-1] if parts else None
    return chosen

# -------------------------------------------------------------------
# Mapeo de columnas canónicas (con alias AT&T)
# -------------------------------------------------------------------
CANON_COLS_MAP = {
    "telefono": "telefono",
    "teléfono": "telefono",

    # --- específicos primero (evitan colisión con "num a") ---
    "numero a imei": "imei",
    "número a imei": "imei",
    "num a imei": "imei",
    "num_a_imei": "imei",         # <- por si viene con guiones bajos
    "imei a": "imei",             # <- alias extra (por seguridad)

    "numero a imsi": "imsi",      # no lo insertas, pero evita colisión
    "número a imsi": "imsi",
    "num a imsi": "imsi",
    "num_a_imsi": "imsi",

    # --- MSISDNs (genéricos) ---
    "numero a": "numero_a",
    "número a": "numero_a",
    "num a": "numero_a",

    "numero b": "numero_b",
    "número b": "numero_b",
    "num b": "numero_b",
    "dest": "numero_b",

    # tipo textual (fallback)
    "tipo": "tipo",

    # fecha/hora/duración
    "fecha": "fecha",
    "hora": "hora",
    "durac": "durac_seg",
    "durac seg": "durac_seg",
    "durac. seg": "durac_seg",
    "duración": "durac_seg",
    "dur": "durac_seg",

    # geografía
    "latitud": "latitud",
    "longitud": "longitud",
    "azimuth": "azimuth",

    # derivación de tipo
    "serv": "serv",
    "t_reg": "t_reg",
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
    df = df.apply(lambda col: col.map(lambda x: x.strip() if isinstance(x, str) else x))
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
# Fecha / hora
# -------------------------------------------------------------------
_FORMATOS_DATETIME = [
    "%d-%m-%y %H:%M:%S",   # Formato de texto: 04-06-25 0:16:06
    "%d-%m-%Y %H:%M:%S",
    "%d/%m/%y %H:%M:%S",
    "%d/%m/%Y %H:%M:%S",
    "%d-%m-%y %H:%M",
    "%d-%m-%Y %H:%M",
    "%d/%m/%y %H:%M",
    "%d/%m/%Y %H:%M",
    "%Y-%m-%d %H:%M:%S",   # Formato ISO cuando Excel serializa fechas
]

def _parse_fecha_hora(fecha: pd.Series, hora: pd.Series) -> pd.Series:
    """
    Parsea fecha y hora de AT&T.
    Maneja dos casos:
    1. Texto: 04-06-25 + 0:16:06
    2. Datetime de Excel: 2024-12-15 00:00:00 + 13:46:08
    """
    # Intentar parsear fecha como datetime primero (puede venir serializada de Excel)
    fecha_dt = pd.to_datetime(fecha, errors='coerce')
    
    # Si la fecha ya es datetime (viene de Excel), extraer solo la parte de fecha
    if fecha_dt.notna().any():
        # Convertir a solo fecha (YYYY-MM-DD)
        f = fecha_dt.dt.strftime('%Y-%m-%d')
    else:
        # Si no es datetime, limpiar como texto
        f = fecha.astype(str).str.strip()
        # Normalizar separadores a guión
        f = f.str.replace(r"[./]", "-", regex=True)
    
    # Limpiar y normalizar hora
    h = hora.astype(str).str.strip()
    
    # Normalizar horas con un solo dígito: "0:16:06" -> "00:16:06"
    def normalize_hour(time_str):
        if pd.isna(time_str) or str(time_str).strip() == "":
            return time_str
        parts = str(time_str).strip().split(":")
        if len(parts) >= 2:
            # Si la hora tiene solo 1 dígito, agregar cero inicial
            if len(parts[0]) == 1:
                parts[0] = parts[0].zfill(2)
            return ":".join(parts)
        return time_str
    
    h = h.apply(normalize_hour)
    
    # Combinar fecha + hora
    combo = (f + " " + h).reset_index(drop=True)
    
    # Inicializar serie de resultados
    ts = pd.Series([pd.NaT] * len(combo), index=combo.index)
    
    # Intentar formatos específicos en orden de prioridad
    for fmt in _FORMATOS_DATETIME:
        try:
            cand = pd.to_datetime(combo, format=fmt, errors="coerce")
            valid_count = cand.notna().sum()
            
            # Si este formato funciona mejor, usarlo
            if valid_count > ts.notna().sum():
                ts = cand
                
            # Si parseamos todo exitosamente, terminar
            if ts.notna().all():
                break
        except Exception:
            continue
    
    # Fallback: usar dayfirst=True para formatos que no matchearon
    if ts.isna().any():
        mask = ts.isna()
        try:
            fallback = pd.to_datetime(combo[mask], dayfirst=True, errors="coerce")
            ts[mask] = fallback
        except Exception as e:
            print(f"Warning: Error en fallback de fecha: {e}")
    
    return ts


# -------------------------------------------------------------------
# Mapeo de tipo (AT&T): (SERV, T_REG) -> entero Enum
# -------------------------------------------------------------------
def _map_tipo_att(serv: Optional[str], t_reg: Optional[str],
                  numero_a: Optional[str] = None, telefono: Optional[str] = None,
                  tipo_textual_fallback: Optional[str] = None) -> int:
    s = _norm(serv or "")
    tr = _norm(t_reg or "")

    def is_ent(x: str) -> bool:
        return x in ("ent", "entrante", "in", "inbound")
    def is_sal(x: str) -> bool:
        return x in ("sal", "saliente", "out", "outbound")

    if s == "data" or s == "datos":
        return TipoRegistroSabana.Datos.value

    if s == "voz" or s == "llamada" or s == "call":
        if is_ent(tr):
            return TipoRegistroSabana.VozEntrante.value
        if is_sal(tr):
            return TipoRegistroSabana.VozSaliente.value
        # inferencia opcional si falta T_REG:
        if numero_a and telefono:
            if str(numero_a) == str(telefono):
                return TipoRegistroSabana.VozSaliente.value
        return TipoRegistroSabana.Ninguno.value

    if s == "sms" or s == "mensaje":
        if is_ent(tr):
            return TipoRegistroSabana.Mensaje2ViasEnt.value
        if is_sal(tr):
            return TipoRegistroSabana.Mensaje2ViasSal.value
        if numero_a and telefono:
            if str(numero_a) == str(telefono):
                return TipoRegistroSabana.Mensaje2ViasSal.value
        return TipoRegistroSabana.Ninguno.value

    if s == "mms":
        return TipoRegistroSabana.MensajeriaMultimedia.value

    # Fallback al mapper textual existente si llega un "tipo" ya armado
    if tipo_textual_fallback:
        t = _norm(tipo_textual_fallback)
        if t.startswith("datos"):
            return TipoRegistroSabana.Datos.value
        if t.startswith("mensaje entrante"):
            return TipoRegistroSabana.Mensaje2ViasEnt.value
        if t.startswith("mensaje saliente"):
            return TipoRegistroSabana.Mensaje2ViasSal.value
        if t.startswith("voz entrante"):
            return TipoRegistroSabana.VozEntrante.value
        if t.startswith("voz saliente"):
            return TipoRegistroSabana.VozSaliente.value
        if t.startswith("voz transfer"):
            return TipoRegistroSabana.VozTransfer.value
        if t.startswith("voz transito"):
            return TipoRegistroSabana.VozTransito.value

    return TipoRegistroSabana.Ninguno.value


# -------------------------------------------------------------------
# Normalización de filas (AT&T)
# -------------------------------------------------------------------
def _frame_to_rows_att(tbl: pd.DataFrame, id_sabanas: int, telefono_archivo: Optional[str]) -> List[Dict]:
    cols = set(map(str, tbl.columns))

    # Fecha/hora
    if "fecha" in cols and "hora" in cols:
        fecha_hora = _parse_fecha_hora(tbl["fecha"], tbl["hora"])
    elif "fecha" in cols:
        f = tbl["fecha"].astype(str).str.strip().str.replace(r"[./]", "-", regex=True)
        fecha_hora = pd.to_datetime(f, dayfirst=True, errors="coerce")
    else:
        fecha_hora = pd.Series([pd.NaT] * len(tbl))

    # Casting y columnas base
    durac    = pd.to_numeric(tbl.get("durac_seg"), errors="coerce")
    numero_a = tbl.get("numero_a")
    numero_b = tbl.get("numero_b")
    lat_raw  = tbl.get("latitud")
    lon_raw  = tbl.get("longitud")
    az_raw   = tbl.get("azimuth")
    imei_raw = tbl.get("imei")
    serv_raw = tbl.get("serv")
    treg_raw = tbl.get("t_reg")
    tipo_raw = tbl.get("tipo")  # textual (opcional)

    # Fallback de IMEI: si no hay columna "imei", busca cualquier columna cuyo encabezado contenga "imei"
    if imei_raw is None:
        for c in tbl.columns:
            if "imei" in _norm(c):
                imei_raw = tbl[c]
                break

    rows: List[Dict] = []
    for i in range(len(tbl)):
        fa = fecha_hora.iloc[i]

        # Duración
        dur = 0
        if durac is not None and not pd.isna(durac.iloc[i]):
            try:
                dur = int(durac.iloc[i])
            except Exception:
                dur = 0

        # MSISDNs
        raw_a = numero_a.iloc[i] if numero_a is not None else None
        raw_b = numero_b.iloc[i] if numero_b is not None else None
        a_clean = _clean_msisdn(raw_a) if raw_a is not None else None
        b_clean = _clean_msisdn(raw_b) if raw_b is not None else None
        a = a_clean if a_clean is not None else (str(raw_a).strip() if raw_a not in (None, "") else None)
        b = b_clean if b_clean is not None else (str(raw_b).strip() if raw_b not in (None, "") else None)

        # telefono = MSISDN del archivo (global)
        tel = telefono_archivo or a or None

        # Tipo (entero) desde SERV/T_REG (o fallback textual)
        serv_val = serv_raw.iloc[i] if serv_raw is not None else None
        treg_val = treg_raw.iloc[i] if treg_raw is not None else None
        tipo_txt = tipo_raw.iloc[i] if tipo_raw is not None else None
        id_tipo = _map_tipo_att(serv_val, treg_val, numero_a=a, telefono=tel, tipo_textual_fallback=tipo_txt)

        # Coordenadas (LAT/LON: mantener política de "último no-cero")
        lat_val  = lat_raw.iloc[i] if lat_raw is not None else None
        lon_val  = lon_raw.iloc[i] if lon_raw is not None else None
        lat_pick = _pick_last_nonzero(lat_val) if lat_val not in (None, "", "NaN") else None
        lon_pick = _pick_last_nonzero(lon_val) if lon_val not in (None, "", "NaN") else None
        lat_d    = _dms_to_decimal(lat_pick) if lat_pick not in (None, "", "NaN") else None
        lon_d    = _dms_to_decimal(lon_pick) if lon_pick not in (None, "", "NaN") else None

        # AZIMUTH "tal como es" (sin elegir último ni modificar):
        # - si viene numérico, se guarda como float;
        # - si viene lista/texto no numérico (p.ej. "[30:40]"), no cabe en double → se deja NULL.
        # az_val = az_raw.iloc[i] if az_raw is not None else None
        # if az_val in (None, "", "NaN"):
        #     az = None
        # else:
        #     s = str(az_val).strip().replace(",", ".")
        #     try:
        #         az = float(s)
        #     except Exception:
        #         az = None
        az_val = az_raw.iloc[i] if az_raw is not None else None
        az = _parse_azimuth(az_val)

        # IMEI
        imei = _clean_imei(imei_raw.iloc[i]) if imei_raw is not None else None

        coord_obj = False if (lat_d is None and lon_d is None) else None

        rows.append({
            "id_sabanas": int(id_sabanas),
            "numero_a": a,
            "numero_b": b,
            "id_tipo_registro": id_tipo,
            "fecha_hora": (fa.to_pydatetime() if isinstance(fa, pd.Timestamp) and not pd.isna(fa) else None),
            "duracion": dur,
            "latitud": str(lat_pick) if lat_pick not in (None, "", "NaN") else None,
            "longitud": str(lon_pick) if lon_pick not in (None, "", "NaN") else None,
            "azimuth": az,  # <-- tal cual venga (si es número)
            "latitud_decimal": float(lat_d) if lat_d is not None else None,
            "longitud_decimal": float(lon_d) if lon_d is not None else None,
            "altitud": 0.0,
            "coordenada_objetivo": coord_obj,
            "imei": imei,
            "telefono": tel,
        })

    # Filtro más estricto (similar a Telcel):
    # Solo conservamos registros que realmente sirven para análisis espacial / co-localización.
    def _is_meaningful_att(r: Dict) -> bool:
        # 1) Debe tener fecha/hora válida
        if r.get("fecha_hora") is None:
            return False

        # 2) Debe tener algún identificador de línea (número A o teléfono)
        if not (r.get("numero_a") or r.get("telefono")):
            return False

        # 3) Debe tener latitud y longitud crudas (texto) para saber que hay coordenadas
        if not r.get("latitud") or not r.get("longitud"):
            return False

        # 4) Debe tener latitud y longitud decimales válidas (no nulas)
        lat_dec = r.get("latitud_decimal")
        lon_dec = r.get("longitud_decimal")
        if lat_dec is None or lon_dec is None:
            return False

        # 5) Debe tener azimuth válido (no nulo, no 0, no vacío)
        az = r.get("azimuth")
        if az is None or az == 0 or az == "":
            return False

        return True

    return [r for r in rows if _is_meaningful_att(r)]



# -------------------------------------------------------------------
# API pública del parser AT&T
# -------------------------------------------------------------------
def run_att_v1_etl(id_sabanas: int, local_path: str, correlation_id: Optional[str] = None) -> int:
    frames = _load_all_sheets(local_path)
    if not frames:
        print(f"[{correlation_id}] No se detectó tabla AT&T en {local_path}")
        return 0

    telefono_archivo = _extract_msisdn_from_filename(local_path)

    all_rows: List[Dict] = []
    for tbl in frames:
        rows = _frame_to_rows_att(tbl, id_sabanas=id_sabanas, telefono_archivo=telefono_archivo)
        all_rows.extend(rows)

    # --- Dedupe ---
    dedup_map: Dict[Tuple, Dict] = {}
    for r in all_rows:
        lat = r.get("latitud")
        lon = r.get("longitud")
        if lat and lon:
            key = (r.get("numero_a"), r.get("fecha_hora"), lat, lon)
        else:
            key = (r.get("numero_a"), r.get("fecha_hora"), r.get("numero_b"))
        if key not in dedup_map:
            dedup_map[key] = r
        else:
            if r.get("duracion", 0) > dedup_map[key].get("duracion", 0):
                dedup_map[key] = r

    all_rows = list(dedup_map.values())

    db = SessionLocal()
    try:
        repo.delete_registros_telefonicos_by_archivo(db, id_sabanas)
        if all_rows:
            repo.insert_registros_telefonicos_bulk(db, all_rows)
        print(f"[{correlation_id}] AT&T v1: insertadas {len(all_rows)} filas (id_sabanas={id_sabanas})")
    except Exception as e:
        print(f"[{correlation_id}] Error en parser AT&T: {e}")
        return -1
    finally:
        db.close()

    if all_rows:
        print(f"[{correlation_id}] DEBUG: total filas parseadas={len(all_rows)}")
        print(f"[{correlation_id}] Ejemplo fila: {all_rows[0]}")

    return len(all_rows)
