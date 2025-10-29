# app/domain/schemas.py
from pydantic import BaseModel, Field
from typing import Optional,List
from datetime import datetime

class JobSabanasRequest(BaseModel):
    id_archivo: int = Field(..., ge=1, description="ID en sabanas.archivos (columna id_sabanas)")

class JobAcceptedResponse(BaseModel):
    job_id: str
    id_archivo: int
    estado: str
    correlation_id: Optional[str] = None

class ErrorResponse(BaseModel):
    detail: str
    code: Optional[str] = None
    
    
class RegistroTelefonicoSchema(BaseModel):
    id_registro_telefonico: int
    id_sabanas: int
    numero_a: Optional[str] = None
    numero_b: Optional[str] = None
    id_tipo_registro: int
    fecha_hora: datetime  # Este será un campo datetime
    duracion: int
    latitud: Optional[str] = None  # Hacer que latitud sea opcional
    longitud: Optional[str] = None  # Hacer que longitud sea opcional
    azimuth: Optional[int] = None
    latitud_decimal: Optional[float] = None  # Hacer que latitud_decimal sea opcional
    longitud_decimal: Optional[float] = None  # Hacer que longitud_decimal sea opcional
    altitud: Optional[int] = None
    coordenada_obtenida: Optional[bool] = None
    imei: Optional[str] = None  # Esto hace que 'imei' sea opcional
    telefono: Optional[str] = None  # Esto hace que 'telefono' sea opcional

    class config:
        orm_mode = True  # Para habilitar la conversión de SQLAlchemy a Pydantic
        from_attributes = True  # Habilitar el uso de from_orm
        anystr_strip_whitespace = True  # Eliminar espacios en blanco en cadenas de texto