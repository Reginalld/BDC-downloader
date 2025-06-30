from pydantic import BaseModel, Field, field_validator
from typing import Optional
from brazil_data_cube.config import SHAPEFILE_PATH, MAX_CLOUD_COVER_DEFAULT
from datetime import datetime


class DownloadRequest(BaseModel):
    satelite: str = Field(pattern=r"^S2_L2A-1$|^landsat-2$", description="Nome do satélite, ex: S2_L2A-1 ou landsat-2")
    
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

    @field_validator("start_date", "end_date")
    @classmethod
    def validate_dates(cls, v):
        try:
            datetime.strptime(v, "%Y-%m-%d")
        except ValueError:
            raise ValueError("A data deve estar no formato YYYY-MM-DD")
        return v

    @field_validator("lat", "lon")
    @classmethod
    def lat_lon_must_be_set_together(cls, v, info):
                # info.field_name vai indicar qual campo está sendo validado ("lat" ou "lon")
        values = info.data  # dados já recebidos

        if info.field_name == "lat" and v is not None and values.get("lon") is None:
            raise ValueError("Se latitude for fornecida, longitude também deve ser.")
        if info.field_name == "lon" and v is not None and values.get("lat") is None:
            raise ValueError("Se longitude for fornecida, latitude também deve ser.")
        return v

