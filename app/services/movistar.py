# app/services/movistar.py
from __future__ import annotations

import math
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

try:
    # Ajusta el import si tu path difiere
    from app.domain import repository
except Exception:  # pragma: no cover
    repository = None  # para que el archivo sea importable sin el repo en tests


# -----------------------------
# Config / Constantes de negocio
# -----------------------------

MOVISTAR_EXPECTED_TOKENS = {
    "TIPO CDR",
    "NUMERO A",
    "NUMERO B",
    "TIPO EVENTO",
    "FECHA EVENTO",
    "HORA EVENTO",
    "DURACION",
    "IMEI",
    "IMSI",
    "codBTS",
    "LATITUD",
    "LONGITUD",
}

# Mapeo a tu catálogo sabanas.ctg_tipo_registro_sabana
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

# Mapeo específico Movistar (TIPO CDR + TIPO EVENTO)
def _map_tipo_registro(tipo_cdr: str, tipo_evento: str) -> int:
    c = (tipo_cdr or "").strip().upper()
    e = (tipo_evento or "").strip().upper()

    if c == "GSM":
        if e == "ENTRANTE":
            return CTG_TIPO_REGISTRO_IDS["VozEntrante"]
        if e == "SALIENTE":
            return CTG_TIPO_REGISTRO_IDS["VozSaliente"]
        return CTG_TIPO_REGISTRO_IDS["Ninguno"]

    if c == "SMS":
        if e == "ENTRANTE":
            return CTG_TIPO_REGISTRO_IDS["Mensaje2ViasEnt"]
        if e == "SALIENTE":
            return CTG_TIPO_REGISTRO_IDS["Mensaje2ViasSal"]
        return CTG_TIPO_REGISTRO_IDS["Ninguno"]

    # Futuras variantes
    return CTG_TIPO_REGISTRO_IDS["Ninguno"]


# -----------------------------
# Utilidades de normalización
# -----------------------------

_DIGITS = re.compile(r"\D+")


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
    return d


def _clean_imei(s: Optional[str]) -> Optional[str]:
    d = _only_digits(s)
    if not d:
        return None
    if len(d) != 15:
        return None
    return d


def _parse_duration_to_seconds(x: Optional[str | int | float]) -> int:
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return 0
    s = str(x).strip()
    if s == "":
        return 0

    # puro entero (ya en segundos)
    if s.isdigit():
        try:
            return int(s)
        except Exception:
            return 0

    # formatos mm:ss o hh:mm:ss
    parts = s.split(":")
    if all(p.isdigit() for p in parts):
        if len(parts) == 2:
            mm, ss = parts
            return int(mm) * 60 + int(ss)
        if len(parts) == 3:
            hh, mm, ss = parts
            return int(hh) * 3600 + int(mm) * 60 + int(ss)

    # último recurso
    try:
        return int(float(s))
    except Exception:
        return 0


def _pad_left(num: int, width: int) -> str:
    s = str(num)
    return s.zfill(width)


def _parse_fecha_hora(
    fecha_raw: Optional[str | int | float],
    hora_raw: Optional[str | int | float],
) -> Optional[datetime]:
    """
    Movistar suele traer FECHA EVENTO como yyyymmdd y HORA EVENTO como hhmmss.
    Aun así, toleramos otras variantes.
    """
    if fecha_raw is None or hora_raw is None:
        return None

    f = str(fecha_raw).strip()
    h = str(hora_raw).strip()

    # Excel numérico (e.g., 20240425 → int/float)
    if f.isdigit() and len(f) in (7, 8):  # a veces se cae un leading zero raro
        f = f.zfill(8)
    if h.isdigit():
        h = _pad_left(int(h), 6)

    # Preferido: yyyymmdd + hhmmss
    try:
        return datetime.strptime(f + h, "%Y%m%d%H%M%S")
    except Exception:
        pass

    # Fallbacks comunes
    # 1) dd/mm/yyyy + hh:mm[:ss]
    try:
        return datetime.strptime(f"{f} {h}", "%d/%m/%Y %H:%M:%S")
    except Exception:
        try:
            return datetime.strptime(f"{f} {h}", "%d/%m/%Y %H:%M")
        except Exception:
            pass

    # 2) dd-mm-yyyy
    for fmt in ("%d-%m-%Y %H:%M:%S", "%d-%m-%Y %H:%M"):
        try:
            return datetime.strptime(f"{f} {h}", fmt)
        except Exception:
            continue

    # 3) Último recurso: pandas
    try:
        return pd.to_datetime(f"{f} {h}", dayfirst=True, errors="coerce").to_pydatetime()
    except Exception:
        return None


_DMS = re.compile(
    r"""^\s*
    (?P<deg>-?\d+(?:[.,]\d+)?)\s*(?:°|º|\s|deg)?
    (?:\s*(?P<min>\d+(?:[.,]\d+)?)\s*(?:'|’|m)?)?
    (?:\s*(?P<sec>\d+(?:[.,]\d+)?)\s*(?:"|”|s)?)?
    \s*(?P<hem>[NSEOWW])?
    \s*$""",
    re.VERBOSE | re.IGNORECASE,
)


def _to_decimal(coord: Optional[str | float | int]) -> Optional[float]:
    """
    Acepta decimal (punto o coma) y DMS con hemisferio.
    """
    if coord is None or (isinstance(coord, float) and math.isnan(coord)):
        return None

    if isinstance(coord, (int, float)):
        return float(coord)

    s = str(coord).strip()
    if s == "":
        return None

    # decimal con coma
    s_dot = s.replace(",", ".")
    try:
        return float(s_dot)
    except Exception:
        pass

    # DMS
    m = _DMS.match(s)
    if m:
        deg = float((m.group("deg") or "0").replace(",", "."))
        minutes = float((m.group("min") or "0").replace(",", "."))
        seconds = float((m.group("sec") or "0").replace(",", "."))
        hem = (m.group("hem") or "").upper()

        sign = 1.0
        if hem in ("S", "W", "O"):  # O = Oeste
            sign = -1.0
        val = sign * (abs(deg) + minutes / 60.0 + seconds / 3600.0)
        return val

    return None


# -----------------------------
# Lectura / detección de cabeceras y bloques
# -----------------------------

def _normalize_colname(c: str) -> str:
    return re.sub(r"\s+", " ", str(c).strip()).upper()


def _find_header_rows(df: pd.DataFrame, max_scan: int = 400, threshold: int = 5) -> List[int]:
    """
    Devuelve TODOS los índices de filas que parecen encabezado.
    Umbral (threshold): número mínimo de tokens esperados presentes en la fila.
    """
    tokens = {t.upper() for t in MOVISTAR_EXPECTED_TOKENS}
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
    """
    Lee todas las hojas y, dentro de cada hoja, TODOS los bloques (si hay varios encabezados).
    Cada bloque se convierte en un DataFrame con columnas normalizadas.
    """
    frames: List[pd.DataFrame] = []

    # CSV/TXT directo
    if path.lower().endswith(".csv") or path.lower().endswith(".txt"):
        df = pd.read_csv(path, dtype=str, engine="python")
        df.columns = [_normalize_colname(c) for c in df.columns]
        frames.append(df)
        return frames

    # Excel con (posibles) múltiples hojas y múltiples bloques por hoja
    xls = pd.read_excel(path, sheet_name=None, dtype=str, header=None, engine="openpyxl")  # type: ignore
    for sheet_name, raw in (xls or {}).items():
        if raw is None or raw.empty:
            continue

        header_idxs = _find_header_rows(raw)
        if not header_idxs:
            continue

        # Añadimos un "final ficticio" para cortar el último bloque
        cut_points = header_idxs + [len(raw)]

        for start_idx, end_idx in zip(cut_points[:-1], cut_points[1:]):
            # Header = fila start_idx; datos = (start_idx + 1) .. (end_idx - 1)
            header_row = start_idx
            data_start = header_row + 1
            data_end = end_idx  # no inclusive en iloc

            if data_start >= data_end:
                continue  # bloque vacío

            block = raw.iloc[data_start:data_end].copy()
            header_values = [_normalize_colname(c) for c in raw.iloc[header_row].tolist()]
            block.columns = header_values

            # Nos quedamos solo con columnas de interés presentes
            keep = [c for c in (_normalize_colname(c) for c in MOVISTAR_EXPECTED_TOKENS) if c in block.columns]
            if not keep:
                continue

            block = block[keep]

            # Quitar filas totalmente vacías
            block = block.replace({np.nan: None})
            block = block.dropna(how="all")
            if block.empty:
                continue

            # (Opcional) Algunas veces el header se repite dentro del bloque: eliminamos esas filas
            def _row_looks_like_header(r) -> bool:
                try:
                    t1 = str(r.get("TIPO CDR", "")).strip().upper() == "TIPO CDR"
                    t2 = str(r.get("NUMERO A", "")).strip().upper() == "NUMERO A"
                    t3 = str(r.get("TIPO EVENTO", "")).strip().upper() == "TIPO EVENTO"
                    return t1 or t2 or t3
                except Exception:
                    return False

            mask_headerlike = block.apply(_row_looks_like_header, axis=1)
            if mask_headerlike.any():
                block = block[~mask_headerlike]

            if not block.empty:
                frames.append(block)

    return frames


# -----------------------------
# Pipeline principal
# -----------------------------

@dataclass
class Stats:
    leidas: int = 0
    validas: int = 0
    descartadas_imei_gsm: int = 0
    descartadas_fecha: int = 0
    descartadas_numero_a: int = 0
    duplicados_con_coords: int = 0
    duplicados_sin_coords: int = 0


def _normalize_rows(df: pd.DataFrame, id_sabanas: int, stats: Stats) -> List[Dict]:
    rows: List[Dict] = []

    # Renombrar a alias internos simples
    alias = {
        "TIPO CDR": "tipo_cdr",
        "NUMERO A": "numero_a",
        "NUMERO B": "numero_b",
        "TIPO EVENTO": "tipo_evento",
        "FECHA EVENTO": "fecha_evento",
        "HORA EVENTO": "hora_evento",
        "DURACION": "duracion",
        "IMEI": "imei",
        "IMSI": "imsi",
        "codBTS": "codbts",
        "LATITUD": "latitud",
        "LONGITUD": "longitud",
    }
    colmap = {_normalize_colname(k): v for k, v in alias.items()}
    df = df.rename(columns=colmap)

    # Asegurar columnas faltantes
    for need in alias.values():
        if need not in df.columns:
            df[need] = None

    stats.leidas += len(df)

    # Limpiar MSISDN y preparar campos base
    df["numero_a_clean"] = df["numero_a"].map(_clean_msisdn)
    df["numero_b_clean"] = df["numero_b"].map(_clean_msisdn)
    df["imei_clean"] = df["imei"].map(_clean_imei)

    # Parse fecha/hora
    df["fecha_hora"] = [
        _parse_fecha_hora(f, h) for f, h in zip(df["fecha_evento"], df["hora_evento"])
    ]

    # Parse duración
    df["duracion_s"] = df["duracion"].map(_parse_duration_to_seconds)

    # Parse coordenadas
    df["lat_dec"] = df["latitud"].map(_to_decimal)
    df["lon_dec"] = df["longitud"].map(_to_decimal)

    # Mapeo tipo
    df["id_tipo_registro"] = [
        _map_tipo_registro(c, e) for c, e in zip(df["tipo_cdr"], df["tipo_evento"])
    ]

    # Reglas de descarte / defaults (ACORDADAS)
    # 1) numero_a obligatorio
    mask_numero_a = df["numero_a_clean"].notna()
    stats.descartadas_numero_a += int((~mask_numero_a).sum())
    df = df[mask_numero_a]

    # 2) fecha_hora obligatoria
    mask_fecha = df["fecha_hora"].notna()
    stats.descartadas_fecha += int((~mask_fecha).sum())
    df = df[mask_fecha]

    # 3) IMEI obligatorio solo para GSM
    is_gsm = df["tipo_cdr"].fillna("").str.strip().str.upper().eq("GSM")
    mask_imei_gsm = (~is_gsm) | df["imei_clean"].notna()
    stats.descartadas_imei_gsm += int((~mask_imei_gsm).sum())
    df = df[mask_imei_gsm]

    # 4) Defaults
    # SMS puede no traer duración → 0
    # Azimuth fijo 360 para todos los registros Movistar
    df["duracion_s"] = df["duracion_s"].fillna(0).astype(int)
    df["azimuth"] = 360
    df["altitud"] = 0.0
    df["coordenada_objetivo"] = np.where(
        df["lat_dec"].notna() & df["lon_dec"].notna(), None, False
    )

    # DEDUP (ACORDADO)
    with_coords = df[df["lat_dec"].notna() & df["lon_dec"].notna()].copy()
    without_coords = df[df["lat_dec"].isna() | df["lon_dec"].isna()].copy()

    # a) con coordenadas
    if not with_coords.empty:
        idx = (
            with_coords.sort_values("duracion_s", ascending=False)
            .groupby(["numero_a_clean", "fecha_hora", "lat_dec", "lon_dec"], dropna=False)
            .head(1)
            .index
        )
        stats.duplicados_con_coords += int(len(with_coords) - len(idx))
        with_coords = with_coords.loc[idx]

    # b) sin coordenadas
    if not without_coords.empty:
        idx2 = (
            without_coords.sort_values("duracion_s", ascending=False)
            .groupby(
                ["numero_a_clean", "numero_b_clean", "fecha_hora", "id_tipo_registro"],
                dropna=False,
            )
            .head(1)
            .index
        )
        stats.duplicados_sin_coords += int(len(without_coords) - len(idx2))
        without_coords = without_coords.loc[idx2]

    final = pd.concat([with_coords, without_coords], axis=0, ignore_index=True)
    stats.validas += len(final)
    
    final = final.sort_values(["fecha_hora", "numero_a_clean", "numero_b_clean"], ascending=[True, True, True]).reset_index(drop=True)

    # Armar registros para inserción (contrato Telcel v1)
    rows: List[Dict] = []
    for r in final.itertuples(index=False):
        rows.append(
            {
                "id_sabanas": id_sabanas,
                "numero_a": r.numero_a_clean,
                "numero_b": r.numero_b_clean,
                "id_tipo_registro": int(r.id_tipo_registro)
                if pd.notna(r.id_tipo_registro)
                else CTG_TIPO_REGISTRO_IDS["Ninguno"],
                "fecha_hora": r.fecha_hora,
                "duracion": int(r.duracion_s) if pd.notna(r.duracion_s) else 0,
                "latitud": r.latitud if pd.notna(r.latitud) else None,
                "longitud": r.longitud if pd.notna(r.longitud) else None,
                "azimuth": int(r.azimuth),
                "latitud_decimal": float(r.lat_dec) if pd.notna(r.lat_dec) else None,
                "longitud_decimal": float(r.lon_dec) if pd.notna(r.lon_dec) else None,
                "altitud": float(r.altitud),
                "coordenada_objetivo": None
                if (pd.notna(r.lat_dec) and pd.notna(r.lon_dec))
                else False,
                "imei": r.imei_clean if pd.notna(r.imei_clean) else None,
                "telefono": r.numero_a_clean,
            }
        )

    return rows


def run_movistar_etl(db_session, id_sabanas: int, file_path: str) -> int:
    """
    Punto de entrada del ETL para Movistar.
    - Lee todas las hojas y todos los bloques (múltiples encabezados)
    - Normaliza y dedup
    - Inserta en sabanas.registros_telefonicos
    - RETORNA: int (filas insertadas) | -1 en error  ← requerido por jobs_service
    """
    try:
        stats = Stats()
        frames = _read_all_sheets(file_path)

        all_rows: List[Dict] = []
        for df in frames:
            if df is None or df.empty:
                continue
            rows = _normalize_rows(df, id_sabanas, stats)
            if rows:
                all_rows.extend(rows)

        # Sin repo (tests/local): devolver conteo como entero
        if repository is None:
            return int(len(all_rows))

        # Borra previos del mismo archivo (como en Telcel v1)
        if hasattr(repository, "delete_registros_telefonicos_by_archivo"):
            repository.delete_registros_telefonicos_by_archivo(db_session, id_sabanas)

        # Inserta bulk
        inserted = 0
        if all_rows and hasattr(repository, "insert_registros_telefonicos_bulk"):
            repository.insert_registros_telefonicos_bulk(db_session, all_rows)
            inserted = len(all_rows)

        return int(inserted)

    except Exception as e:
        # Log opcional:
        print(f"[MOVISTAR ETL] Error id={id_sabanas}: {e}")
        return -1
