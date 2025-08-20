# app/domain/schemas.py
from pydantic import BaseModel, Field
from typing import Optional

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
