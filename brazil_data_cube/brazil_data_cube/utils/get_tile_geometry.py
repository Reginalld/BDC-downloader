import logging
from typing import Optional

import fiona
import geopandas as gpd
import pandas as pd
from shapely.geometry import shape


class GeometryLoader:
    def __init__(self, logger: logging.Logger, tile_grid_path: str):
        self.logger = logger
        self.tile_grid_path = tile_grid_path

    def get_tile_geometry(self, tile_id: str, satellite: str):
        """
        Lê a geometria correta do tile no SHP, respeitando as regras
        de Sentinel-2, Landsat, CBERS e Sentinel-1 do BDC.
        """

        try:
            with fiona.open(self.tile_grid_path, 'r') as collection:
                custom_crs = collection.crs_wkt
                records = [
                    {
                        'properties': rec['properties'],
                        'geometry': shape(rec['geometry'])
                    }
                    for rec in collection
                ]
        except Exception as e:
            self.logger.error(f"Erro ao carregar grade {self.tile_grid_path}: {e}")
            return None

        attrs = pd.DataFrame([rec['properties'] for rec in records])
        geoms = gpd.GeoSeries([rec['geometry'] for rec in records], crs=custom_crs)
        tiles_gdf = gpd.GeoDataFrame(attrs, geometry=geoms)

        tile_row = None

        sat = satellite.upper()

        if "S2A" in sat:
            tile_row = tiles_gdf[tiles_gdf["NAME"] == tile_id]

        elif sat == "L8":
            # Landsat PATH/ROW (ex: "227067")
            try:
                path = int(tile_id[:3])
                row = int(tile_id[3:])
                tile_row = tiles_gdf[(tiles_gdf["PATH"] == path) & (tiles_gdf["ROW"] == row)]
            except ValueError:
                self.logger.error(f"Tile Landsat inválido: {tile_id}")
                return None

        elif "CB" in sat.upper() or "S1A" in sat:
            tile_row = tiles_gdf[tiles_gdf["tile"] == tile_id]

        if tile_row is None or tile_row.empty:
            self.logger.warning(f"Tile {tile_id} não encontrado no SHP.")
            return None

        return tile_row.iloc[0].geometry
