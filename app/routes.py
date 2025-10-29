# app/routes.py
from decouple import config
from fastapi import (
    APIRouter,
    BackgroundTasks,
    Depends,
    Header,
    HTTPException,
    status,
)

from app.domain.schemas import JobSabanasRequest, JobAcceptedResponse
from app.jobs_service import accept_job_sabana, process_job_sabana  # <- importa funciones del módulo RENOMBRADO

# Conexion con base de datos y schema
from app.database import SessionLocal
from app.domain.models import RegistroTelefonico  # Modelo SQLAlchemy
from app.domain.schemas import RegistroTelefonicoSchema  # Esquema Pydantic para la respuesta
from typing import Optional,List
from sqlalchemy.orm import Session
from app.database import get_db


router = APIRouter(prefix="/jobs", tags=["jobs"])

# decouple lee el .env automáticamente
SERVICE_API_KEY = config("SERVICE_API_KEY", default="")

def require_api_key(
    x_api_key: Optional[str] = Header(default=None),
    authorization: Optional[str] = Header(default=None),
):
    """
    Valida la API key de servicio. Acepta:
      - x-api-key: <clave>
      - Authorization: Bearer <clave>
    """
    expected = SERVICE_API_KEY
    token = x_api_key or (authorization.replace("Bearer ", "") if authorization else None)

    if not expected or not token or token.strip() != expected.strip():
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Auth inválida",
        )

@router.post(
    "/sabanas",
    response_model=JobAcceptedResponse,
    status_code=status.HTTP_202_ACCEPTED,
    responses={
        401: {"description": "Unauthorized"},
        404: {"description": "Not Found"},
        409: {"description": "Conflict"},
    },
)
def enqueue_sabana_job(
    payload: JobSabanasRequest,
    background: BackgroundTasks,
    _auth=Depends(require_api_key),
    idempotency_key: Optional[str] = Header(default=None, alias="Idempotency-Key"),
    correlation_id: Optional[str] = Header(default=None, alias="X-Correlation-ID"),
):
    """
    Acepta un job para analizar una sábana:
      - Cambia estado: subido -> en_cola (transición atómica)
      - Devuelve 202 Accepted
      - Lanza tarea en background para: en_cola -> procesando + descarga FTP
    """
    job_id, _row = accept_job_sabana(payload.id_archivo)

    # Lanza el worker en background (descarga desde FTP). En esta etapa
    # no cerramos a "procesado", solo demostramos la transición + descarga.
    background.add_task(
        process_job_sabana,
        payload.id_archivo,
        correlation_id,
        False,  # mark_processed_after_download
    )

    return JobAcceptedResponse(
        job_id=job_id,
        id_archivo=payload.id_archivo,
        estado="en_cola",
        correlation_id=correlation_id,
    )


# @router.get("/registros/{id_sabanas}", response_model=List[RegistroTelefonicoSchema])  # Usa el esquema Pydantic
# async def obtener_registros_telefonicos(id_sabanas: int, db: Session = Depends(get_db)):
#     # Hacer la consulta con SQLAlchemy
#     registros_db = db.query(RegistroTelefonico).filter(RegistroTelefonico.id_sabanas == id_sabanas).all()

#     if not registros_db:
#         raise HTTPException(status_code=404, detail="No se encontraron registros para este id_sabana")

#     # Convertir los resultados de SQLAlchemy a objetos Pydantic
#     registros = [RegistroTelefonicoSchema.from_orm(registro) for registro in registros_db]
    
#     return registros