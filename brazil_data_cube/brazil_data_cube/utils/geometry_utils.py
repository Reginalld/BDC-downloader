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

        if satellite == "S2":
            tile_row = tiles_gdf[tiles_gdf["NAME"] == tile_id]
        else:
            path = int(tile_id[:3])
            row = int(tile_id[3:])
            tile_row = tiles_gdf[
                (tiles_gdf["PATH"] == path) & (tiles_gdf["ROW"] == row)
            ]

        if tile_row.empty:
            self.logger.warning(
                f"Tile {tile_id} não encontrado na grade do Sentinel-2."
            )
            return False

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
