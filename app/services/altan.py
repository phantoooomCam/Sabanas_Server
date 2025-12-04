# app/services/altan.py
from __future__ import annotations

import math
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

try:
    from app.domain import repository
except Exception:  # pragma: no cover
    repository = None


# =========================
# Config / Catálogos
# =========================

ALTAN_EXPECTED_TOKENS = {
    "TIPO DE COMUNICACIÓN",
    "NÚMERO ORIGEN",
    "NÚMERO DESTINO",
    "DURACIÓN",
    "FECHA DE LA COMUNICACIÓN",
    "HORA DE LA COMUNICACIÓN",
    "ETIQUETA DE LOCALIZACIÓN",
    "LATITUD",
    "LONGITUD",
    "IMEI",
    "IMSI",
}

CTG_TIPO_REGISTRO_IDS: Dict[str, int] = {
    "Datos": 0,
    "MensajeriaMultimedia": 1,
    "Mensaje2ViasEnt": 2,
    "Mensaje2ViasSal": 3,
    "VozEntrante": 4,
    "VozSaliente": 5,
    "VozTransfer": 6,
    "VozTransito": 7,
    "Ninguno": 8,
    "Wifi": 9,
    "ReenvioSal": 10,
    "ReenvioEnt": 11,
}

# Tipos que reporta ALTÁN
ALTAN_TIPOS = {
    "VOZ": "VOZ",
    "DATOS": "DATOS",
    "SERVICIO DE MENSAJE CORTO": "SMS",
    "SERVICIO SUPLEMENTARIO DE REENVIO": "REENVIO",
}


# =========================
# Utilidades
# =========================

_DIGITS = re.compile(r"\D+")


def _normalize_colname(c: str) -> str:
    # Normaliza espacios y mayúsculas (conserva tildes)
    return re.sub(r"\s+", " ", str(c).strip()).upper()


def _only_digits(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    s = str(s).strip()
    if not s:
        return None
    return _DIGITS.sub("", s)


def _is_all_zeros(s: Optional[str]) -> bool:
    if not s:
        return True
    return all(ch == "0" for ch in s)


def _clean_msisdn(s: Optional[str]) -> Optional[str]:
    d = _only_digits(s)
    if not d or _is_all_zeros(d):
        return None
    # >10 dígitos → nos quedamos con los últimos 10; <10 → se conserva
    if len(d) > 10:
        d = d[-10:]
    return d


def _clean_imei(s: Optional[str]) -> Optional[str]:
    d = _only_digits(s)
    if not d:
        return None
    if len(d) != 15:
        return None
    return d


def _pad_left(num: int, width: int) -> str:
    return str(num).zfill(width)


def _parse_duration_to_seconds(x: Optional[str | int | float]) -> int:
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return 0
    s = str(x).strip()
    if s == "":
        return 0
    if s.isdigit():
        return int(s)
    parts = s.split(":")
    if all(p.isdigit() for p in parts):
        if len(parts) == 2:
            mm, ss = parts
            return int(mm) * 60 + int(ss)
        if len(parts) == 3:
            hh, mm, ss = parts
            return int(hh) * 3600 + int(mm) * 60 + int(ss)
    try:
        return int(float(s))
    except Exception:
        return 0


def _parse_fecha_hora(
    fecha_raw: Optional[str | int | float],
    hora_raw: Optional[str | int | float],
) -> Optional[datetime]:
    """
    Altán suele traer fecha dd/mm/yyyy y hora hh:mm(:ss),
    pero aceptamos variantes y también yyyymmdd + hhmmss.
    """
    if fecha_raw is None or hora_raw is None:
        return None

    f = str(fecha_raw).strip()
    h = str(hora_raw).strip()

    # Si hora viene sin separadores y es numérica → 6 dígitos padded
    if h.isdigit():
        h = _pad_left(int(h), 6)

    # 1) dd/mm/yyyy + hh:mm(:ss)
    for fmt in ("%d/%m/%Y %H:%M:%S", "%d/%m/%Y %H:%M"):
        try:
            return datetime.strptime(f"{f} {h}", fmt)
        except Exception:
            pass

    # 2) yyyymmdd + hhmmss
    if f.isdigit():
        f2 = f.zfill(8)
        try:
            return datetime.strptime(f2 + h, "%Y%m%d%H%M%S")
        except Exception:
            pass

    # 3) fallback robusto con pandas
    try:
        return pd.to_datetime(f"{f} {h}", dayfirst=True, errors="coerce").to_pydatetime()
    except Exception:
        return None


_DMS = re.compile(
    r"""^\s*
    (?P<deg>-?\d+(?:[.,]\d+)?)\s*(?:°|º|\s|deg)?
    (?:\s*(?P<min>\d+(?:[.,]\d+)?)\s*(?:'|’|m)?)?
    (?:\s*(?P<sec>\d+(?:[.,]\d+)?)\s*(?:"|”|s)?)?
    \s*(?P<hem>[NSEOWO])?
    \s*$""",
    re.VERBOSE | re.IGNORECASE,
)


def _to_decimal(coord: Optional[str | float | int]) -> Optional[float]:
    if coord is None or (isinstance(coord, float) and math.isnan(coord)):
        return None
    if isinstance(coord, (int, float)):
        val = float(coord)
        # Verificar si es NaN
        if math.isnan(val):
            return None
        return val

    s = str(coord).strip().lower()
    if s in ("", "nan", "none", "null", "na", "n/a"):
        return None

    # decimal con coma
    s_dot = s.replace(",", ".")
    try:
        val = float(s_dot)
        # Verificar si es NaN
        if math.isnan(val):
            return None
        return val
    except Exception:
        pass

    # DMS
    m = _DMS.match(s)
    if m:
        deg = float((m.group("deg") or "0").replace(",", "."))
        minutes = float((m.group("min") or "0").replace(",", "."))
        seconds = float((m.group("sec") or "0").replace(",", "."))
        hem = (m.group("hem") or "").upper()
        sign = -1.0 if hem in ("S", "W", "O") else 1.0
        return sign * (abs(deg) + minutes / 60.0 + seconds / 3600.0)

    return None


# =========================
# Lectura multi-hoja/bloque
# =========================

def _find_header_rows(df: pd.DataFrame, max_scan: int = 400, threshold: int = 5) -> List[int]:
    tokens = {t.upper() for t in ALTAN_EXPECTED_TOKENS}
    max_rows = min(len(df), max_scan)

    header_idxs: List[int] = []
    for i in range(max_rows):
        row_vals = {
            _normalize_colname(x)
            for x in df.iloc[i].tolist()
            if str(x).strip() != ""
        }
        score = len(tokens.intersection(row_vals))
        if score >= threshold:
            header_idxs.append(i)
    return header_idxs


def _read_all_sheets(path: str) -> List[pd.DataFrame]:
    frames: List[pd.DataFrame] = []

    if path.lower().endswith(".csv") or path.lower().endswith(".txt"):
        df = pd.read_csv(path, dtype=str, engine="python")
        df.columns = [_normalize_colname(c) for c in df.columns]
        frames.append(df)
        return frames

    xls = pd.read_excel(path, sheet_name=None, dtype=str, header=None, engine="openpyxl")  # type: ignore
    for _, raw in (xls or {}).items():
        if raw is None or raw.empty:
            continue

        header_idxs = _find_header_rows(raw)
        if not header_idxs:
            continue

        cut_points = header_idxs + [len(raw)]
        for start_idx, end_idx in zip(cut_points[:-1], cut_points[1:]):
            header_row = start_idx
            data_start = header_row + 1
            data_end = end_idx
            if data_start >= data_end:
                continue

            block = raw.iloc[data_start:data_end].copy()
            header_vals = [_normalize_colname(c) for c in raw.iloc[header_row].tolist()]
            block.columns = header_vals

            keep = [c for c in (_normalize_colname(c) for c in ALTAN_EXPECTED_TOKENS) if c in block.columns]
            if not keep:
                continue

            block = block[keep].replace({np.nan: None}).dropna(how="all")
            if block.empty:
                continue

            # Quitar posibles reiteraciones de encabezado dentro del bloque
            def _row_is_header_like(r) -> bool:
                try:
                    return str(r.get("TIPO DE COMUNICACIÓN", "")).strip().upper() == "TIPO DE COMUNICACIÓN"
                except Exception:
                    return False

            mask_headerlike = block.apply(_row_is_header_like, axis=1)
            if mask_headerlike.any():
                block = block[~mask_headerlike]

            if not block.empty:
                frames.append(block)

    return frames


# =========================
# Normalización principal
# =========================

@dataclass
class Stats:
    leidas: int = 0
    validas: int = 0
    descartadas_numero_a: int = 0
    descartadas_geo: int = 0
    descartadas_imei_voz: int = 0
    duplicados: int = 0


def _infer_provider_type(s: Optional[str]) -> str:
    s2 = (s or "").strip().upper()
    return ALTAN_TIPOS.get(s2, "OTRO")


def _estimate_subscriber(df: pd.DataFrame) -> Optional[str]:
    """
    Estimamos el MSISDN del abonado como el Número Origen más frecuente.
    """
    a = df.get("NÚMERO ORIGEN", df.get("NUMERO ORIGEN"))  # tolera con/sin acento
    if a is None:
        return None
    a = a.dropna().astype(str)
    if a.empty:
        return None
    a = a.map(_clean_msisdn)
    a = a[a.notna()]
    if a.empty:
        return None
    mode = a.mode()
    return mode.iloc[0] if not mode.empty else None


def _map_tipo_registro_altan(tipo_com: str, dir_flag: str) -> int:
    """
    dir_flag: 'ENTRANTE' | 'SALIENTE' | '' (desconocido)
    """
    t = _infer_provider_type(tipo_com)
    d = (dir_flag or "").upper()

    if t == "DATOS":
        return CTG_TIPO_REGISTRO_IDS["Datos"]

    if t == "SMS":
        if d == "ENTRANTE":
            return CTG_TIPO_REGISTRO_IDS["Mensaje2ViasEnt"]
        if d == "SALIENTE":
            return CTG_TIPO_REGISTRO_IDS["Mensaje2ViasSal"]
        return CTG_TIPO_REGISTRO_IDS["Ninguno"]

    if t == "REENVIO":
        if d == "ENTRANTE":
            return CTG_TIPO_REGISTRO_IDS["ReenvioEnt"]
        if d == "SALIENTE":
            return CTG_TIPO_REGISTRO_IDS["ReenvioSal"]
        return CTG_TIPO_REGISTRO_IDS["Ninguno"]

    if t == "VOZ":
        if d == "ENTRANTE":
            return CTG_TIPO_REGISTRO_IDS["VozEntrante"]
        if d == "SALIENTE":
            return CTG_TIPO_REGISTRO_IDS["VozSaliente"]
        return CTG_TIPO_REGISTRO_IDS["Ninguno"]

    return CTG_TIPO_REGISTRO_IDS["Ninguno"]


def _infer_direction(row: pd.Series, abonado: Optional[str]) -> str:
    if not abonado:
        return ""
    a = _clean_msisdn(row.get("NÚMERO ORIGEN"))
    b = _clean_msisdn(row.get("NÚMERO DESTINO"))
    if a == abonado and (b is None or b != abonado):
        return "SALIENTE"
    if b == abonado and (a is None or a != abonado):
        return "ENTRANTE"
    return ""


def _normalize_block(df_block: pd.DataFrame, id_sabanas: int, stats: Stats) -> List[Dict]:
    # Estandarizar nombres exactamente como esperamos
    df = df_block.copy()
    df.columns = [_normalize_colname(c) for c in df.columns]

    # Asegurar columnas (por si alguna viene ausente)
    for c in ALTAN_EXPECTED_TOKENS:
        if c not in df.columns:
            df[c] = None

    stats.leidas += len(df)

    # Estimar MSISDN del abonado
    abonado = _estimate_subscriber(df)

    # Limpiezas base y parseos
    df["numero_a_clean"] = df["NÚMERO ORIGEN"].map(_clean_msisdn)
    df["numero_b_clean"] = df["NÚMERO DESTINO"].map(_clean_msisdn)
    df["imei_clean"] = df["IMEI"].map(_clean_imei)

    df["fecha_hora"] = [
        _parse_fecha_hora(f, h)
        for f, h in zip(df["FECHA DE LA COMUNICACIÓN"], df["HORA DE LA COMUNICACIÓN"])
    ]
    df["duracion_s"] = df["DURACIÓN"].map(_parse_duration_to_seconds)

    df["lat_dec"] = df["LATITUD"].map(_to_decimal)
    df["lon_dec"] = df["LONGITUD"].map(_to_decimal)

    # Dirección (cuando aplique) e id_tipo_registro
    df["dir_inferida"] = df.apply(lambda r: _infer_direction(r, abonado), axis=1)
    df["id_tipo_registro"] = [
        _map_tipo_registro_altan(t, d)
        for t, d in zip(df["TIPO DE COMUNICACIÓN"], df["dir_inferida"])
    ]

    # ===== Filtros acordados =====
    # 1) Número Origen obligatorio
    mask_a = df["numero_a_clean"].notna()
    stats.descartadas_numero_a += int((~mask_a).sum())
    df = df[mask_a]

    # 2) Geo obligatoria (lat/lon válidas)
    mask_geo = df["lat_dec"].notna() & df["lon_dec"].notna()
    stats.descartadas_geo += int((~mask_geo).sum())
    df = df[mask_geo]

    # 3) IMEI obligatorio solo para VOZ (⚠️ usar .str, no .strip de Python)
    is_voz_row = df["TIPO DE COMUNICACIÓN"].fillna("").str.strip().str.upper().eq("VOZ")
    mask_imei_voz = (~is_voz_row) | df["imei_clean"].notna()
    stats.descartadas_imei_voz += int((~mask_imei_voz).sum())
    df = df[mask_imei_voz]

    # Defaults
    df["duracion_s"] = df["duracion_s"].fillna(0).astype(int)
    df["azimuth"] = 360
    df["altitud"] = 0.0
    df["coordenada_objetivo"] = None  # siempre hay geo tras el filtro

    # ===== Deduplicación =====
    # Clave más específica para no colapsar eventos distintos que comparten A/fecha/geo
    if not df.empty:
        sorted_df = df.sort_values("duracion_s", ascending=False)

        group_cols = [
            "numero_a_clean",
            "numero_b_clean",
            "id_tipo_registro",
            "fecha_hora",
            "lat_dec",
            "lon_dec",
        ]

        idx = (
            sorted_df.groupby(group_cols, dropna=False)
            .head(1)
            .index
        )
        stats.duplicados += int(len(sorted_df) - len(idx))
        df = sorted_df.loc[idx]

    # ===== Orden de salida cronológica =====
    df = df.sort_values(["fecha_hora", "numero_a_clean", "numero_b_clean"]).reset_index(drop=True)

    # ===== Armar registros para inserción =====
    rows: List[Dict] = []
    for r in df.itertuples(index=False):
        rows.append(
            {
                "id_sabanas": id_sabanas,
                "numero_a": r.numero_a_clean,
                "numero_b": r.numero_b_clean,
                "id_tipo_registro": int(r.id_tipo_registro)
                if pd.notna(r.id_tipo_registro)
                else CTG_TIPO_REGISTRO_IDS["Ninguno"],
                "fecha_hora": r.fecha_hora,
                "duracion": int(r.duracion_s),
                "latitud": getattr(r, "LATITUD", None),
                "longitud": getattr(r, "LONGITUD", None),
                "azimuth": int(r.azimuth),
                "latitud_decimal": float(r.lat_dec) if pd.notna(r.lat_dec) else None,
                "longitud_decimal": float(r.lon_dec) if pd.notna(r.lon_dec) else None,
                "altitud": float(r.altitud),
                "coordenada_objetivo": None,  # hay geo
                "imei": r.imei_clean if pd.notna(r.imei_clean) else None,
                "telefono": r.numero_a_clean,
            }
        )

    stats.validas += len(rows)
    return rows


def run_altan_etl(db_session, id_sabanas: int, file_path: str) -> int:
    """
    ETL para ALTÁN:
      - lee todas las hojas y bloques
      - normaliza, filtra y deduplica
      - inserta en DB
      - retorna int (filas insertadas) o -1 en error (para jobs_service)
    """
    try:
        frames = _read_all_sheets(file_path)
        stats = Stats()

        all_rows: List[Dict] = []
        for block in frames:
            if block is None or block.empty:
                continue
            rows = _normalize_block(block, id_sabanas, stats)
            if rows:
                all_rows.extend(rows)

        if repository is None:
            print(
                f"[ALTAN ETL] id={id_sabanas} leidas={stats.leidas} "
                f"validas={stats.validas} desc_numA={stats.descartadas_numero_a} "
                f"desc_geo={stats.descartadas_geo} desc_imei_voz={stats.descartadas_imei_voz} "
                f"duplicados={stats.duplicados}"
            )
            return int(len(all_rows))

        # Limpieza previa (mismo archivo)
        if hasattr(repository, "delete_registros_telefonicos_by_archivo"):
            repository.delete_registros_telefonicos_by_archivo(db_session, id_sabanas)

        inserted = 0
        if all_rows and hasattr(repository, "insert_registros_telefonicos_bulk"):
            repository.insert_registros_telefonicos_bulk(db_session, all_rows)
            inserted = len(all_rows)

        print(
            f"[ALTAN ETL] id={id_sabanas} leidas={stats.leidas} "
            f"validas={stats.validas} desc_numA={stats.descartadas_numero_a} "
            f"desc_geo={stats.descartadas_geo} desc_imei_voz={stats.descartadas_imei_voz} "
            f"duplicados={stats.duplicados} inserted={inserted}"
        )

        return int(inserted)
    except Exception as e:
        print(f"[ALTAN ETL] Error id={id_sabanas}: {e}")
        return -1
