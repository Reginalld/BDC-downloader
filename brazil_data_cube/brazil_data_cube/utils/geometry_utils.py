# brazil_data_cube/downloader/geometry_utils.py

import geopandas as gpd
from shapely.geometry import shape
import logging
from typing import Any

logger = logging.getLogger(__name__)

class GeometryUtils:
    def __init__(self, tile_grid_path: str):
        self.tile_grid_path = tile_grid_path

    def is_good_geometry(self, item: Any, tile_id: str, satelite: str) -> bool:
        """
        Valida se a imagem cobre mais de 82% de geometria do tile especificado.
        
        Args:
            item (Any): Item STAC retornado pelo catálogo
            tile_id (str): ID do tile Sentinel-2

        Returns:
            bool: True se passou no teste, False caso contrário
        """
        tiles_gdf = gpd.read_file(self.tile_grid_path)
        if satelite == "S2-L2A_1":
            tile_row = tiles_gdf[tiles_gdf["NAME"] == tile_id]
        else:
            path = int(tile_id[:3])
            row = int(tile_id[3:])
            tile_row = tiles_gdf[(tiles_gdf["PATH"] == path) & (tiles_gdf["ROW"] == row)]

        if tile_row.empty:
            logger.warning(f"Tile {tile_id} não encontrado na grade do Sentinel-2.")
            return False

        tile_geom = tile_row.iloc[0].geometry
        item_geom = shape(item.geometry)
        intersection = tile_geom.intersection(item_geom)

        if intersection.area / tile_geom.area >= 0.82:
            return True
        else:
            logger.debug(f"Imagem fora do tile {tile_id} - área de interseção insuficiente.")
            return False