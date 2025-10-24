# app/domain/repository.py
from sqlalchemy import text
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

# Lee un registro por id_sabanas
def get_archivo_by_id(db, id_archivo: int):
    sql = text("""
        SELECT id_sabanas, ruta, estado, fecha_inicio, fecha_termino, compania,
               id_numero_telefonico, id_compania_telefonica
        FROM sabanas.archivos
        WHERE id_sabanas = :id
        LIMIT 1
    """)
    row = db.execute(sql, {"id": id_archivo}).mappings().first()
    return dict(row) if row else None

# Transición condicional de estado (control de concurrencia)
def try_mark_estado(db, id_archivo: int, expected: str, new_state: str,
                    set_inicio: bool = False, set_termino: bool = False) -> bool:
    sets = ["estado = :new_state"]
    params = {"id": id_archivo, "expected": expected, "new_state": new_state}
    if set_inicio:
        sets.append("fecha_inicio = :now")
        params["now"] = datetime.utcnow()
    if set_termino:
        sets.append("fecha_termino = :now")
        params["now"] = datetime.utcnow()

    sql = text(f"""
        UPDATE sabanas.archivos
        SET {", ".join(sets)}
        WHERE id_sabanas = :id AND estado = :expected
    """)
    res = db.execute(sql, params)
    db.commit()
    return res.rowcount == 1

# Marcar error simple (opcional)
def mark_error(db, id_archivo: int):
    sql = text("""
        UPDATE sabanas.archivos
        SET estado = 'error', fecha_termino = :now
        WHERE id_sabanas = :id
    """)
    db.execute(sql, {"id": id_archivo, "now": datetime.utcnow()})
    db.commit()


# Insertar en BLOQUE

def delete_registros_telefonicos_by_archivo(db, id_sabanas: int) -> int:
    sql = text("""
        DELETE FROM sabanas.registros_telefonicos
        WHERE id_sabanas = :id_sabanas
    """)
    try:
        with db.begin():
            res = db.execute(sql, {"id_sabanas": id_sabanas})
            return res.rowcount
    except Exception:
        logger.exception("delete_registros_telefonicos_by_archivo failed for id_sabanas=%s", id_sabanas)
        raise

def insert_registros_telefonicos_bulk(db, rows: list[dict]) -> int:
    if not rows:
        return 0

    sql = text("""
        INSERT INTO sabanas.registros_telefonicos (
            id_sabanas, numero_a, numero_b, id_tipo_registro, fecha_hora,
            duracion, latitud, longitud, azimuth, latitud_decimal,
            longitud_decimal, altitud, coordenada_objetivo, imei, telefono
        ) VALUES (
            :id_sabanas, :numero_a, :numero_b, :id_tipo_registro, :fecha_hora,
            :duracion, :latitud, :longitud, :azimuth, :latitud_decimal,
            :longitud_decimal, :altitud, :coordenada_objetivo, :imei, :telefono
        )
    """)

    normalized = []
    now = datetime.utcnow()
    threshold_year = now.year + 1

    for r in rows:
        rr = dict(r)
        fh = rr.get("fecha_hora")

        # Asegurar tipo y formato consistente antes de enviar al driver
        if fh is None:
            rr["fecha_hora"] = None
        else:
            # si es string, intentar parsear a datetime; si es datetime, formatear como SQL-safe string
            if isinstance(fh, str):
                fh_str = fh.strip()
                if fh_str == "":
                    rr["fecha_hora"] = None
                else:
                    # preferir iso / YYYY-MM-DD HH:MM:SS
                    try:
                        parsed = datetime.fromisoformat(fh_str)
                    except Exception:
                        try:
                            parsed = datetime.strptime(fh_str, "%Y-%m-%d %H:%M:%S")
                        except Exception:
                            logger.warning("fecha_hora no parseable, leaving string as-is: %r", fh_str)
                            parsed = None
                    if parsed:
                        rr["fecha_hora"] = parsed.strftime("%Y-%m-%d %H:%M:%S")
            else:
                # objeto datetime-like
                try:
                    rr["fecha_hora"] = fh.strftime("%Y-%m-%d %H:%M:%S")
                except Exception:
                    # último recurso: convertir usando ISO
                    try:
                        rr["fecha_hora"] = str(fh)
                    except Exception:
                        rr["fecha_hora"] = None

        # Validación de rango: evitar insertar años claramente erróneos
        fh_val = rr.get("fecha_hora")
        if isinstance(fh_val, str) and fh_val:
            try:
                year = int(fh_val[:4])
                if year > threshold_year or year < 1970:
                    logger.error("fila con fecha fuera de rango detectada; id_sabanas=%s fecha_hora=%s", rr.get("id_sabanas"), fh_val)
                    raise ValueError(f"fecha_hora fuera de rango: {fh_val}")
            except Exception:
                # si no podemos evaluar el año, abortar para evitar corrupción silenciosa
                logger.exception("No se pudo validar fecha_hora: %r", fh_val)
                raise

        # normalizar cadenas vacías a None para campos optativos
        for k in ("imei", "coordenada_objetivo", "latitud", "longitud", "telefono", "numero_b"):
            v = rr.get(k)
            if isinstance(v, str) and v.strip() == "":
                rr[k] = None

        normalized.append(rr)

    try:
        with db.begin():
            db.execute(sql, normalized)
        return len(normalized)
    except Exception:
        logger.exception("insert_registros_telefonicos_bulk failed (rows=%d)", len(normalized))
        raise
