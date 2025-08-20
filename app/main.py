# app/main.py
from fastapi import FastAPI
from app.routes import router as jobs_router

app = FastAPI(title="Sabanas Server")

app.include_router(jobs_router)

# healthcheck simple
@app.get("/health")
def health():
    return {"ok": True}
