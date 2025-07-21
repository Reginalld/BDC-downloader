from pydantic import BaseModel, Field, field_validator, model_validator
from typing import Optional
from brazil_data_cube.config import SHAPEFILE_PATH, MAX_CLOUD_COVER_DEFAULT, SAT_SUPPORTED, TILES_PARANA, LANDSAT_TILES_PARANA, TILES_PATH_LANDSAT, TILES_PATH_SENTINEL
from datetime import datetime
import json

with open(TILES_PATH_SENTINEL, "r", encoding="utf-8") as f:
    TILES_SENTINEL_POR_UF = json.load(f)

with open(TILES_PATH_LANDSAT, "r", encoding="utf-8") as f:
    TILES_LANDSAT_POR_UF = json.load(f)

class DownloadRequest(BaseModel):

    satellite: str 
    lat: Optional[float] = Field(None, ge=-90.0, le=90.0)
    lon: Optional[float] = Field(None, ge=-180.0, le=180.0)
    tile_id: Optional[str] = Field(
        None,
        min_length=2,
        max_length=2,
        description="Sigla do estado brasileiro (ex: 'PR', 'SP')"
    )
    radius_km: Optional[float] = Field(10.0, ge=0.1, le=100.0)
    start_date: str
    end_date: str
    tile_grid_path: str = SHAPEFILE_PATH
    max_cloud_cover: float = MAX_CLOUD_COVER_DEFAULT

    @field_validator("satellite")
    @classmethod
    def validate_sat(cls, v):
        if v not in SAT_SUPPORTED:
            raise ValueError(f"Satélite '{v}' não suportado. Escolha entre: {SAT_SUPPORTED}")
        return v
    
    @field_validator("tile_id")
    @classmethod
    def validate_tile_id(cls, v):
        if v is None:
            return v

        v_up = v.strip().upper()

        valid_ufs = set(TILES_SENTINEL_POR_UF.keys()) | set(TILES_LANDSAT_POR_UF.keys())
        if v_up not in valid_ufs:
            raise ValueError(f"Sigla de estado '{v_up}' inválida ou sem tiles disponíveis. Use uma destas: {sorted(valid_ufs)}")

        return v_up

    @field_validator("start_date", "end_date")
    @classmethod
    def validate_dates(cls, v):
        try:
            datetime.strptime(v, "%Y-%m-%d")
        except ValueError:
            raise ValueError("A data deve estar no formato YYYY-MM-DD")
        return v
    
    @model_validator(mode="after")
    def validate_date_range(self):
        if datetime.strptime(self.start_date, "%Y-%m-%d") > datetime.strptime(self.end_date, "%Y-%m-%d"):
            raise ValueError("A data de início deve ser anterior ou igual à data de término")
        return self
    
    @model_validator(mode="after")
    def validate_lat_lon_and_id(self):
        if self.tile_id and (self.lat or self.lon):
            raise ValueError("Informe apenas 'tile_id' ou par de coordenadas (lat/lon), não ambos.")

        if (self.lat is not None and self.lon is None) or (self.lon is not None and self.lat is None):
            raise ValueError("Latitude e longitude devem ser fornecidas juntas.")

        return self
    
    @model_validator(mode="after")
    def validate_id_for_sat(self):
        if self.tile_id in LANDSAT_TILES_PARANA:
            if self.satellite == "S2_L2A-1":
                raise ValueError("ID não compatível com o satélite, você quis dizer landsat-2?.")
            
        if self.tile_id in TILES_PARANA:
            if self.satellite == "landsat-2":
                raise ValueError("ID não compatível com o satélite, você quis dizer S2_L2A-1?.")
        return self

