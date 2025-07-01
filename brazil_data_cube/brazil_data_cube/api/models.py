from pydantic import BaseModel, Field, field_validator, model_validator
from typing import Optional
from brazil_data_cube.config import SHAPEFILE_PATH, MAX_CLOUD_COVER_DEFAULT, SAT_SUPPORTED
from datetime import datetime


class DownloadRequest(BaseModel):
    satelite: str 
    
    lat: Optional[float] = Field(None, ge=-90.0, le=90.0)
    lon: Optional[float] = Field(None, ge=-180.0, le=180.0)
    tile_id: Optional[str] = Field(
        None,
        min_length=5,
        max_length=6,
        pattern=r"^[0-9]{2}[A-Z]{3}$|^parana$",
        description="Tile Sentinel-2 ou 'parana'"
    )
    radius_km: Optional[float] = Field(10.0, ge=0.1, le=100.0)
    start_date: str
    end_date: str
    tile_grid_path: str = SHAPEFILE_PATH
    max_cloud_cover: float = MAX_CLOUD_COVER_DEFAULT

    @field_validator("satelite")
    @classmethod
    def validate_sat(cls, v):
        if v != "S2_L2A-1" and v != "landsat-2":
            raise ValueError(f"Satélite {v} não suportado, escolha entre: {tuple(SAT_SUPPORTED)}")
        return v

    @field_validator("start_date", "end_date")
    @classmethod
    def validate_dates(cls, v):
        try:
            datetime.strptime(v, "%Y-%m-%d")
        except ValueError:
            raise ValueError("A data deve estar no formato YYYY-MM-DD")
        return v
    
    @model_validator(mode="after")
    def lat_lon_do_not_be_set_if_id(self):
        if (self.lat is not None or self.lon is not None ):
            if(self.tile_id is not None):
                raise ValueError("É permitido apenas permitido um parâmetro de busca, escolha entre coordenadas ou ID")

        return self
        
    @model_validator(mode="after")
    def lat_lon_must_be_set_together(self):
        if (self.lat is not None and self.lon is None) or (self.lon is not None and self.lat is None):
            raise ValueError("Latitude e longitude devem ser informadas juntas.")
        return self

