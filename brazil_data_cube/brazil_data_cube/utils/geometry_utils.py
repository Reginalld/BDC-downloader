import logging
from typing import Any

import geopandas as gpd
from shapely.geometry import shape


class GeometryUtils:
    def __init__(self, logger: logging.Logger, tile_grid_path: str):
        self.tile_grid_path = tile_grid_path
        self.logger = logger

    def is_good_geometry(
        self,
        item: Any,
        tile_id: str,
        satellite: str,
        min_geometry_cover: float
    ) -> bool:
        """
        Valida se a imagem cobre mais de 82% de geometria do tile especificado.

        Args:
            item (Any): Item STAC retornado pelo catálogo
            tile_id (str): ID do tile Sentinel-2

        Returns:
            bool: True se passou no teste, False caso contrário
        """
        tiles_gdf = gpd.read_file(self.tile_grid_path)
        tile_row = None

        if satellite == "S2":
            # Sentinel-2 usa campo NAME
            tile_row = tiles_gdf[tiles_gdf["NAME"] == tile_id]

        elif satellite == "LANDSAT":
            # Landsat usa PATH e ROW (ex: "227067")
            path = int(tile_id[:3])
            row = int(tile_id[3:])
            tile_row = tiles_gdf[
                (tiles_gdf["PATH"] == path) & (tiles_gdf["ROW"] == row)
            ]

        elif satellite == "CBERS4-MUX-2M-1":  
            normalized_tile_id = tile_id.replace('_', '/')
            tile_row = tiles_gdf[tiles_gdf["Name"] == normalized_tile_id]

        if tile_row.empty:
            self.logger.warning(f"Tile {tile_id} não encontrado na grade {satellite}.")
            return False

        # Geometrias
        tile_geom = tile_row.iloc[0].geometry
        item_geom = shape(item.geometry)
        intersection = tile_geom.intersection(item_geom)

        percentage_geometry = min_geometry_cover / 100

        if intersection.area / tile_geom.area >= percentage_geometry:
            return True

        self.logger.debug(
            f"Imagem fora do tile {tile_id} - área de interseção insuficiente."
        )
        return False