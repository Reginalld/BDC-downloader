import json
from datetime import datetime
from typing import Optional

from brazil_data_cube.config import (MAX_CLOUD_COVER_DEFAULT, SAT_SUPPORTED,
                                     SHAPEFILE_PATH, TILES_PATH_LANDSAT, 
                                     TILES_PATH_SENTINEL)
from pydantic import BaseModel, Field, field_validator, model_validator

with open(TILES_PATH_SENTINEL, "r", encoding="utf-8") as f:
    TILES_SENTINEL_POR_UF = json.load(f)

with open(TILES_PATH_LANDSAT, "r", encoding="utf-8") as f:
    TILES_LANDSAT_POR_UF = json.load(f)

DATE_FORMAT = "%Y-%m-%d"

UFS_SENTINEL = set(TILES_SENTINEL_POR_UF.keys())
UFS_LANDSAT = set(TILES_LANDSAT_POR_UF.keys())
ALL_VALID_UFS = UFS_SENTINEL | UFS_LANDSAT

# Conjuntos com TODOS os IDs de tiles válidos para cada satélite
ALL_SENTINEL_TILES = {tile for tiles_list in TILES_SENTINEL_POR_UF.values() for tile in tiles_list}
ALL_LANDSAT_TILES = {tile for tiles_list in TILES_LANDSAT_POR_UF.values() for tile in tiles_list}
ALL_VALID_TILES = ALL_SENTINEL_TILES | ALL_LANDSAT_TILES

class DownloadRequest(BaseModel):
    satellite: str
    lat: Optional[float] = Field(None, ge=-90.0, le=90.0)
    lon: Optional[float] = Field(None, ge=-180.0, le=180.0)
    tile_id: Optional[str] = Field(
        None,
        min_length=2,
        max_length=6,
        description="Sigla do estado brasileiro (ex: 'PR', 'SP')"
    )
    radius_km: Optional[float] = Field(10.0, ge=0.1, le=100.0)
    start_date: str
    end_date: str
    tile_grid_path: str = SHAPEFILE_PATH
    max_cloud_cover: float = MAX_CLOUD_COVER_DEFAULT

    @field_validator("tile_id")
    @classmethod
    def validate_tile_id(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        
        v_up = v.strip().upper()

        # Verifica se o ID é uma UF válida OU um tile válido
        if v_up not in ALL_VALID_UFS and v_up not in ALL_VALID_TILES:
            raise ValueError(f"ID de localização '{v_up}' inválido ou sem tiles disponíveis.")
        
        return v_up

    @field_validator("start_date", "end_date")
    @classmethod
    def validate_dates(cls, v):
        try:
            datetime.strptime(v, DATE_FORMAT)
        except ValueError:
            raise ValueError("A data deve estar no formato YYYY-MM-DD")
        return v

    @model_validator(mode="after")
    def validate_date_range(self):
        start = datetime.strptime(self.start_date, DATE_FORMAT)
        end = datetime.strptime(self.end_date, DATE_FORMAT)

        if start > end:
            raise ValueError(
                "A data de início deve ser anterior ou igual à data de término"
            )
        return self

    @model_validator(mode="after")
    def validate_lat_lon_and_id(self):
        if self.tile_id and (self.lat or self.lon):
            raise ValueError(
                "Informe apenas 'tile_id' ou par de"
                "coordenadas (lat/lon), não ambos."
                )

        if (self.lat is not None) != (self.lon is not None):
            raise ValueError(
                "Latitude e longitude devem ser fornecidas juntas."
                )

        return self

    @model_validator(mode="after")
    def validate_location_input(self):
        # Validador que garante que ou temos tile_id ou lat/lon, mas não ambos
        has_tile_id = self.tile_id is not None
        has_lat_lon = self.lat is not None and self.lon is not None

        if has_tile_id and (self.lat or self.lon):
            raise ValueError("Informe apenas 'tile_id' ou o par de coordenadas (lat/lon), não ambos.")
        
        if (self.lat is not None) != (self.lon is not None):
            raise ValueError("Latitude e longitude devem ser fornecidas juntas.")
        
        if not has_tile_id and not has_lat_lon:
            raise ValueError("É necessário fornecer 'tile_id' ou um par de coordenadas (lat/lon).")

        return self

    @model_validator(mode="after")
    def validate_location_and_satellite_compatibility(self):
        # Validador corrigido que checa a compatibilidade entre o ID e o satélite
        if self.tile_id is None:
            return self

        # Caso 1: O ID é um tile específico
        if self.tile_id in ALL_SENTINEL_TILES and self.satellite == "landsat-2":
            raise ValueError(f"O tile '{self.tile_id}' é do Sentinel, mas o satélite selecionado é landsat-2.")
        
        if self.tile_id in ALL_LANDSAT_TILES and self.satellite == "S2_L2A-1":
            raise ValueError(f"O tile '{self.tile_id}' é do Landsat, mas o satélite selecionado é S2_L2A-1.")

        # Caso 2: O ID é uma sigla de estado (UF)
        if self.tile_id in ALL_VALID_UFS:
            if self.satellite == "S2_L2A-1" and self.tile_id not in UFS_SENTINEL:
                raise ValueError(f"O estado '{self.tile_id}' não possui tiles disponíveis para o satélite S2_L2A-1.")
            
            if self.satellite == "landsat-2" and self.tile_id not in UFS_LANDSAT:
                raise ValueError(f"O estado '{self.tile_id}' não possui tiles disponíveis para o satélite landsat-2.")
        
        return self
