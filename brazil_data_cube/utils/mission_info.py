# brazil_data_cube/downloader/mission_info.py

import json

from brazil_data_cube.config import (SHAPEFILE_PATH_SENTINEL, SHAPEFILE_PATH_BDC_MD,
                                     SHAPEFILE_PATH_LANDSAT,
                                     TILES_PATH_BDC_MD_V2, TILES_PATH_LANDSAT,
                                     TILES_PATH_SENTINEL)

# Carregamento único dos tiles por UF
with open(TILES_PATH_LANDSAT, "r", encoding="utf-8") as f:
    LANDSAT_TILES = json.load(f)

with open(TILES_PATH_SENTINEL, "r", encoding="utf-8") as f:
    SENTINEL_TILES = json.load(f)

with open(TILES_PATH_BDC_MD_V2, "r", encoding="utf-8") as f:
    CBERS_TILES = json.load(f)


class MissionInfo:
    """Centraliza as informações específicas de cada missão/satélite."""

    def __init__(self, satellite: str):
        sat_lower = satellite.strip().lower()

        if "l8" in sat_lower:
            self.tiles_por_uf = LANDSAT_TILES
            self.tile_grid_path = SHAPEFILE_PATH_LANDSAT
            self.bucket_prefix = "landsat"
            self.mission = "LANDSAT"
            self.sat = "L8"
            self.level = "LEVEL2"

        elif "s2" in sat_lower:
            self.tiles_por_uf = SENTINEL_TILES
            self.tile_grid_path = SHAPEFILE_PATH_SENTINEL
            self.bucket_prefix = "s2"
            self.mission = "SENTINEL2"
            self.sat = "S2A"
            self.level = "L2A"

        elif "cb" in sat_lower:
            self.tiles_por_uf = CBERS_TILES
            self.tile_grid_path = SHAPEFILE_PATH_BDC_MD
            self.bucket_prefix = "cbers"
            self.mission = "CBERS"
            self.sat = "CB4"
            self.level = "SR"

        elif "s1" in sat_lower:
            self.tiles_por_uf = CBERS_TILES
            self.tile_grid_path = SHAPEFILE_PATH_BDC_MD
            self.bucket_prefix = "s1"
            self.mission = "SENTINEL1"
            self.sat = "S1A"
            self.level = "SAR"

        else:
            raise ValueError(f"Satélite não reconhecido: {sat_lower}")
