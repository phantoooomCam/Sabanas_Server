from fastapi import APIRouter
from .database import engine
from sqlalchemy import text

router = APIRouter()

@router.get("/ping")
def ping():
    return {"message":"Microservicio Corriendo..."}



@router.get("/db-ping")
def db_ping():
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version();"))
            version = result.fetchone()
            return {"status": "ok", "version": version[0]}
    except Exception as e:
        return {"status": "error", "detail": str(e)}