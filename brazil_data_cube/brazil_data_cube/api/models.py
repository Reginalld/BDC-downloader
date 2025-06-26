from pydantic import BaseModel
from typing import Optional
from brazil_data_cube.config import SHAPEFILE_PATH, MAX_CLOUD_COVER_DEFAULT


class DownloadRequest(BaseModel):
    satelite: str
    lat: Optional[float] = None
    lon: Optional[float] = None
    tile_id: Optional[str] = None
    radius_km: Optional[float] = 10.0
    start_date: str
    end_date: str
    tile_grid_path: str = SHAPEFILE_PATH
    max_cloud_cover: float = MAX_CLOUD_COVER_DEFAULT
