# app/routes.py
import os
import uuid
from fastapi import APIRouter, Depends, Header, BackgroundTasks, HTTPException, status
from app.domain.schemas import JobSabanasRequest, JobAcceptedResponse
from app import services

router = APIRouter(prefix="/jobs", tags=["jobs"])

SERVICE_API_KEY = os.getenv("SERVICE_API_KEY", "")

def require_api_key(x_api_key: str | None = Header(default=None), authorization: str | None = Header(default=None)):
    # Muy simple para pruebas. En producción, usa JWT de servicio/verificación robusta.
    token = x_api_key or (authorization.replace("Bearer ", "") if authorization else None)
    if not SERVICE_API_KEY or token != SERVICE_API_KEY:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Auth inválida")

@router.post("/sabanas", response_model=JobAcceptedResponse, status_code=status.HTTP_202_ACCEPTED,
             responses={401: {"description": "Unauthorized"}, 404: {"description": "Not Found"}, 409: {"description": "Conflict"}})
def enqueue_sabana_job(
    payload: JobSabanasRequest,
    background: BackgroundTasks,
    _auth=Depends(require_api_key),
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    correlation_id: str | None = Header(default=None, alias="X-Correlation-ID"),
):
    # (Opcional) idempotencia en memoria solo para dev
    # En prod: guarda Idempotency-Key en DB/redis.
    job_id, row = services.accept_job_sabana(payload.id_archivo)
    # Lanza worker en background para esta prueba: hará en_cola -> procesando + descarga
    background.add_task(services.process_job_sabana, payload.id_archivo, correlation_id, False)

    return JobAcceptedResponse(
        job_id=job_id,
        id_archivo=payload.id_archivo,
        estado="en_cola",
        correlation_id=correlation_id
    )
