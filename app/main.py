from fastapi import FastAPI
from . import routes

app = FastAPI(title="Microservicio Sabanas")

app.include_router(routes.router)