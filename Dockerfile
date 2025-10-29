# Imagen para Python con versión estable
FROM python:3.13.1-slim

# Establecer directorio de trabajo
WORKDIR /app

# Copiar solo requirements primero (para aprovechar cache)
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt 
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev

# Copiar el código de la aplicación (esto ya incluye todo)
COPY . .

# Comando para ejecutar la aplicación
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
