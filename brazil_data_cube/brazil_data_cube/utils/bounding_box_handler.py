import logging
import math
import os
from typing import List, Optional, Tuple

import fiona
import geopandas as gpd
import pandas as pd
from shapely.geometry import MultiPolygon, Polygon, shape


class BoundingBoxHandler:
    def __init__(self, logger: logging.Logger, reduction_factor: float = 0.2):
        self.reduction_factor = reduction_factor
        self.logger = logger

    def calculate_reduced_bbox(
        self, tile_grid: any
    ) -> List[float]:
        """
        Calcula uma bounding box reduzida com
        base nas coordenadas de um bbox existente.
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

        self.logger.info(
            f"Main_bbox ajustado: "
            f"[{new_minx}, {new_miny}, {new_maxx}, {new_maxy}]"
        )
        return [new_minx, new_miny, new_maxx, new_maxy]

    def calculate_reduced_bbox_tile(
        self, minx: float, miny: float, maxx: float, maxy: float
    ) -> List[float]:
        """
        Calcula uma bounding box reduzida com base
        nas coordenadas de um bbox existente.
        -- REFATORADA: Agora aceita coordenadas diretas
        para uma melhor modularidade.
        """
        center_x = (minx + maxx) / 2
        center_y = (miny + maxy) / 2

        width = (maxx - minx) * 0.005
        height = (maxy - miny) * 0.005

        new_minx = center_x - (width / 2)
        new_maxx = center_x + (width / 2)
        new_miny = center_y - (height / 2)
        new_maxy = center_y + (height / 2)

        self.logger.info(
            f"Main_bbox ajustado: "
            f"[{new_minx}, {new_miny}, {new_maxx}, {new_maxy}]"
        )
        return [new_minx, new_miny, new_maxx, new_maxy]

    @staticmethod
    def to_2d(geom):
        """Converte Polygon ou MultiPolygon com Z para 2D."""
        if geom.has_z:
            if isinstance(geom, Polygon):
                exterior = [(x, y) for x, y, *rest in geom.exterior.coords]
                interiors = [
                    [(x, y) for x, y, *rest in ring.coords]
                    for ring in geom.interiors
                ]
                return Polygon(exterior, interiors)
            elif isinstance(geom, MultiPolygon):
                # Recursively apply to_2d to each polygon in the multipolygon
                return MultiPolygon(
                    [BoundingBoxHandler.to_2d(p) for p in geom.geoms]
                    )
        return geom

    def obter_bounding_box(
        self,
        tile_id: Optional[str],
        lat: Optional[float],
        lon: Optional[float],
        radius_km: float,
        tile_grid_path: str,
        satellite: str
    ) -> Tuple[List[float], float, float, float]:
        """
        Gera uma bounding box com base em tile_id ou coordenadas.
        """
        if tile_id:
            if not os.path.isfile(tile_grid_path):
                msg = f"Arquivo Shapefile não encontrado: {tile_grid_path}"
                self.logger.error(msg)
                raise FileNotFoundError(msg)

            try:
                self.logger.info(f"Carregando grade com "
                                 f"projeção customizada: {tile_grid_path}")
                with fiona.open(tile_grid_path, 'r') as collection:
                    # Lê a definição da projeção (WKT) diretamente do arquivo
                    custom_crs_wkt = collection.crs_wkt
                    # Lê as geometrias e atributos
                    records = [
                        {'properties': rec['properties'],
                         'geometry': shape(rec['geometry'])}
                        for rec in collection
                        ]

                # Monta o GeoDataFrame manualmente para evitar o erro de CRS
                attrs = pd.DataFrame([rec['properties'] for rec in records])
                geoms = gpd.GeoSeries([rec['geometry'] for rec in records],
                                      crs=custom_crs_wkt)
                tile_grid = gpd.GeoDataFrame(attrs, geometry=geoms)
                self.logger.info(
                    f"Grade '{os.path.basename(tile_grid_path)}' "
                    f"carregada com sucesso ({len(tile_grid)} tiles)."
                                 )
            except Exception as e:
                self.logger.error(f"Falha ao carregar a "
                                  f"grade de tiles '{tile_grid_path}': {e}")
                raise  # Re-lança a exceção para parar a execução

            if tile_grid.crs and tile_grid.crs.to_epsg() != 4326:
                self.logger.info(f"Convertendo CRS de "
                                 f"{tile_grid.crs} para EPSG:4326.")
                tile_grid = tile_grid.to_crs(epsg=4326)

            if "CB" in satellite:
                normalized_tile_id = tile_id.replace("_", "/")
                tile_data = tile_grid[tile_grid["tile"] == normalized_tile_id]
            elif satellite == "S2":
                tile_data = tile_grid[tile_grid["NAME"] == tile_id]
            else:
                path = int(tile_id[:3])
                row = int(tile_id[3:])
                tile_data = tile_grid[
                    (tile_grid["PATH"] == path) & (tile_grid["ROW"] == row)
                    ]

            if tile_data.empty:
                msg = f"Tile {tile_id} não encontrado "
                f"na grade do satélite {satellite}."
                self.logger.error(msg)
                raise ValueError(msg)

            tile_geometry_3d = tile_data.geometry.iloc[0]
            tile_geometry_2d = self.to_2d(tile_geometry_3d)
            minx, miny, maxx, maxy = tile_geometry_2d.bounds

            if "CB" not in satellite:
                main_bbox = self.calculate_reduced_bbox_tile(
                    minx, miny, maxx, maxy)
            else:
                main_bbox = [minx, miny, maxx, maxy]

            lat = (miny + maxy) / 2
            lon = (minx + maxx) / 2
            bbox_width_km = (maxx - minx) * \
                111.32 * math.cos(math.radians(lat))
            bbox_height_km = (maxy - miny) * 111.32
            radius_km = max(bbox_width_km, bbox_height_km) / 2

        elif lat is not None and lon is not None:
            from .bounding_box_calculator import BoundingBoxCalculator
            main_bbox = BoundingBoxCalculator.calculate(lat, lon, radius_km)
            self.logger.info("Processando sem tile ID.")
        else:
            msg = "É necessário fornecer latitude/longitude ou um ID de tile."
            self.logger.error(msg)
            raise ValueError(msg)

        self.logger.info(f"BBox principal: {main_bbox}")
        return main_bbox, lat, lon, radius_km
