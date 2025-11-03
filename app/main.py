# app/main.py
from fastapi import FastAPI
from app.routes import router as jobs_router
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="Sabanas Server")

app.include_router(jobs_router)

ALLOWED_ORIGINS = [
    "http://localhost:5173",
    "http://127.0.0.1:5173",
    "http://192.168.100.92:5173",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,  # no uses "*" si allow_credentials=True
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
    expose_headers=["*"],
)

# healthcheck simple
@app.get("/health")
def health():
    return {"ok": True}
