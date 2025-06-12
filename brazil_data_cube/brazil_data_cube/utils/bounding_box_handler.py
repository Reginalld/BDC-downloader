# brazil_data_cube/bounding_box_handler.py

import math
import logging
from ..config import REDUCTION_FACTOR
import os

from typing import List, Optional, Tuple


logger = logging.getLogger(__name__)

class BoundingBoxHandler:
    def __init__(self, reduction_factor: float = 0.2):
        self.reduction_factor = reduction_factor

    def calcular_bbox_reduzido(self, tile_grid: any) -> List[float]:
        """
        Calcula uma bounding box reduzida com base na geometria do tile.
        
        Args:
            tile_grid (GeoDataFrame): Geometria do tile
            
        Returns:
            List[float]: [minx, miny, maxx, maxy] da nova bbox
        """
        tile_geometry = tile_grid.geometry.iloc[0]
        minx, miny, maxx, maxy = tile_geometry.bounds

        center_x = (minx + maxx) / 2
        center_y = (miny + maxy) / 2
        width = (maxx - minx) * self.reduction_factor
        height = (maxy - miny) * self.reduction_factor

        new_minx = center_x - (width / 2)
        new_maxx = center_x + (width / 2)
        new_miny = center_y - (height / 2)
        new_maxy = center_y + (height / 2)

        logger.info(f"Main_bbox ajustado: [{new_minx}, {new_miny}, {new_maxx}, {new_maxy}]")
        return [new_minx, new_miny, new_maxx, new_maxy]

    def obter_bounding_box(self, tile_id: Optional[str], lat: Optional[float],
                           lon: Optional[float], radius_km: float,
                           tile_grid_path: str) -> Tuple[List[float], float, float, float]:
        """
        Gera uma bounding box com base em tile_id ou coordenadas.
        
        Args:
            tile_id (Optional[str]): ID do tile (ex: '21JYM')
            lat (Optional[float]): Latitude central
            lon (Optional[float]): Longitude central
            radius_km (float): Raio em km
            tile_grid_path (str): Caminho do shapefile com grade

        Returns:
            Tuple[List[float], float, float, float]: BBox, lat_final, lon_final, radius_final
        """
        if tile_id:
            import geopandas as gpd
            if not os.path.isfile(tile_grid_path):
                logging.error(f"Arquivo Shapefile não encontrado: {tile_grid_path}")
                raise FileNotFoundError(f"Shapefile não encontrado no caminho: {tile_grid_path}")
            
            tile_grid = gpd.read_file(tile_grid_path)
            tile_grid = tile_grid[tile_grid["NAME"] == tile_id]

            if tile_grid.empty:
                logger.error(f"Tile {tile_id} não encontrado na grade Sentinel-2.")
                raise ValueError(f"Tile ID inválido: {tile_id}")

            tile_geometry = tile_grid.geometry.iloc[0]
            minx, miny, maxx, maxy = tile_geometry.bounds

            center_x = (minx + maxx) / 2
            center_y = (miny + maxy) / 2
            width = (maxx - minx) * self.reduction_factor
            height = (maxy - miny) * self.reduction_factor

            new_minx = center_x - (width / 2)
            new_maxx = center_x + (width / 2)
            new_miny = center_y - (height / 2)
            new_maxy = center_y + (height / 2)

            main_bbox = [new_minx, new_miny, new_maxx, new_maxy]

            lat = (miny + maxy) / 2
            lon = (minx + maxx) / 2
            bbox_width_km = ((maxx - minx) * 111 * math.cos(math.radians(lat)))
            bbox_height_km = ((maxy - miny) * 111)
            radius_km = max(bbox_width_km, bbox_height_km) / 2

        elif lat is not None and lon is not None:
            from .bounding_box_calculator import BoundingBoxCalculator
            main_bbox = BoundingBoxCalculator.calcular(lat, lon, radius_km)
            logger.info("Processando sem tile ID.")
        else:
            logger.error("É necessário fornecer latitude/longitude ou um ID de tile Sentinel-2.")
            raise ValueError("Faltam parâmetros para definir a área de interesse.")

        logger.info(f"BBox principal: {main_bbox}")
        return main_bbox, lat, lon, radius_km