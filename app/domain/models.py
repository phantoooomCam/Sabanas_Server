# app/domain/models.py
from sqlalchemy import Column, Integer, String, Float
from app.database import Base

class RegistroTelefonico(Base):
    __tablename__ = 'registros_telefonicos'  # Nombre de la tabla
    __table_args__ = {'schema': 'sabanas'}  # Especifica el esquema 'sabanas'

    id_registro_telefonico = Column(Integer, primary_key=True, index=True)
    id_sabanas = Column(Integer)
    numero_a = Column(String, nullable=True)
    numero_b = Column(String, nullable=True)
    id_tipo_registro = Column(Integer)
    fecha_hora = Column(String)
    duracion = Column(Integer)
    latitud = Column(String)
    longitud = Column(String)
    azimuth = Column(Integer, nullable=True)
    latitud_decimal = Column(Float)
    longitud_decimal = Column(Float)
    altitud = Column(Integer, nullable=True)
    coordenada_objetivo = Column(Integer, nullable=True)
    imei = Column(String, nullable=True)  # Asegúrate de que esta columna esté en la base de datos
    telefono = Column(String, nullable=True)  # Asegúrate de que esta columna esté en la base de datos
