from geoalchemy2 import Geometry
from sqlalchemy import Column, Date, DateTime, Integer, String, func
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, unique=True, index=True, nullable=False)
    hashed_password = Column(String, nullable=False)


class SatelliteScene(Base):
    __tablename__ = 'satellite_scenes'

    id = Column(Integer, primary_key=True, index=True)
    filename = Column(String, unique=True, nullable=False)

    mission = Column(String, nullable=False, index=True)
    satellite = Column(String, nullable=True)
    tile_id = Column(String, index=True)
    date = Column(Date, nullable=False, index=True)
    minio_path = Column(String, nullable=False)
    band = Column(String, index=True, nullable=True)

    geometry = Column(Geometry('POLYGON', srid=4326))

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    edited_at = Column(DateTime(timezone=True),
                       server_default=func.now(), onupdate=func.now())
