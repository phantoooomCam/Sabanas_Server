# app/domain/repository.py
from sqlalchemy import text
from datetime import datetime

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
