import logging
import math
import os
from typing import List, Optional, Tuple

import fiona
import geopandas as gpd
import pandas as pd
from shapely.geometry import MultiPolygon, Polygon, shape


class BoundingBoxHandler:
    """
    Utilitário de manipulação e cálculo de geometrias espaciais.

    Esta classe centraliza a lógica matemática para converter Identificadores de Tile
    (ex: 'T22KGA') ou Coordenadas Geográficas (Lat/Lon) em Bounding Boxes (BBox)
    precisos, prontos para serem usados em consultas espaciais STAC.

    Destaques Técnicos:
    - **Margem de Segurança:** Aplica um fator de redução no BBox para evitar
      erros de ponto flutuante na interseção de bordas.
    - **Sanitização 3D->2D:** Converte geometrias com eixo Z (altitude) para 2D,
      garantindo compatibilidade com operações planas do Shapely/PostGIS.

    Attributes:
        logger (logging.Logger): Logger configurado.
        reduction_factor (float): Fator percentual de redução do BBox (padrão 0.5%).
    """
    def __init__(
            self, logger: logging.Logger, reduction_factor: float = 0.005):
        self.reduction_factor = reduction_factor
        self.logger = logger

    def calculate_reduced_bbox(
        self, tile_grid: any
    ) -> List[float]:
        """
        Calcula um BBox muito menor que o original a partir de um GeoDataFrame.

        Usado para evitar falsos positivos em buscas espaciais, onde a borda de um tile
        pode tocar acidentalmente o tile vizinho.

        Args:
            tile_grid (gpd.GeoDataFrame): DataFrame contendo a geometria do tile.

        Returns:
            List[float]: BBox reduzido [minx, miny, maxx, maxy].
        """

        # Extrai os limites da primeira geometria
        tile_geometry = tile_grid.geometry.iloc[0]
        minx, miny, maxx, maxy = tile_geometry.bounds

        # Calcula o centroide
        center_x = (minx + maxx) / 2
        center_y = (miny + maxy) / 2

        # Aplica o fator de redução
        width = (maxx - minx) * self.reduction_factor
        height = (maxy - miny) * self.reduction_factor

        # Recalcula limites contraídos
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
        Versão escalar do cálculo de BBox reduzido.

        Aceita coordenadas diretas (floats), útil quando não se tem um objeto GeoPandas.

        Args:
            minx (float): Longitude mínima.
            miny (float): Latitude mínima.
            maxx (float): Longitude máxima.
            maxy (float): Latitude máxima.

        Returns:
            List[float]: BBox reduzido [minx, miny, maxx, maxy].
        """
        center_x = (minx + maxx) / 2
        center_y = (miny + maxy) / 2

        # Usa fator fixo de 0.005 aqui(poderia usar self.reduction_factor, porém foram feitos testes com outros valores, por isso variável fixa).
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
        """
        Converte geometrias espaciais 3D (com Z) para 2D.

        Muitos shapefiles brutos trazem altitude (Z=0). Isso causa erros em
        bibliotecas que esperam apenas (X, Y). Esta função remove a terceira dimensão.

        Args:
            geom (Union[Polygon, MultiPolygon]): Geometria Shapely.

        Returns:
            Union[Polygon, MultiPolygon]: Nova geometria puramente 2D.
        """
        if geom.has_z:
            if isinstance(geom, Polygon):
                # Reconstrói polígono ignorando o terceiro elemento da tupla (Z)
                exterior = [(x, y) for x, y, *rest in geom.exterior.coords]
                interiors = [
                    [(x, y) for x, y, *rest in ring.coords]
                    for ring in geom.interiors
                ]
                return Polygon(exterior, interiors)
            elif isinstance(geom, MultiPolygon):
                # Aplica recursivamente para cada polígono do MultiPolygon
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
        Método Mestre para resolução de geometria.

        Decide se deve calcular o BBox baseando-se em um ID de Tile (carregando Shapefile)
        ou em um ponto Lat/Lon (calculando buffer circular).

        Args:
            tile_id (Optional[str]): ID do Tile (ex: 'T22KGA').
            lat (Optional[float]): Latitude.
            lon (Optional[float]): Longitude.
            radius_km (float): Raio de busca.
            tile_grid_path (str): Caminho do Shapefile.
            satellite (str): Nome do satélite.

        Returns:
            Tuple: (BBox [minx, miny, maxx, maxy], lat, lon, radius_km).

        Raises:
            ValueError: Se tile não for encontrado ou input for inválido.
        """
        if tile_id:
            # Estratégia 1: Busca baseada em Tile ID
            tile_grid = self.load_tile_grid(tile_grid_path)
            tile_data = self.get_tile_data(tile_grid, tile_id, satellite)

            if tile_data.empty:
                msg = f"Tile {tile_id} não encontrado na "
                f"grade do satélite {satellite}."
                self.logger.error(msg)
                raise ValueError(msg)

            main_bbox, lat, lon, radius_km = self.calculate_tile_bbox(
                tile_data, satellite
            )

        elif lat is not None and lon is not None:
            # Estratégia 2: Busca baseada em Coordenadas
            main_bbox = self.calculate_bbox_from_coords(lat, lon, radius_km)
        else:
            msg = "É necessário fornecer latitude/longitude ou um ID de tile."
            self.logger.error(msg)
            raise ValueError(msg)

        self.logger.info(f"BBox principal: {main_bbox}")
        return main_bbox, lat, lon, radius_km

    def load_tile_grid(self, tile_grid_path: str) -> gpd.GeoDataFrame:
        """
        Carrega o Shapefile de referência na memória.

        Args:
            tile_grid_path (str): Caminho absoluto do arquivo .shp.

        Returns:
            gpd.GeoDataFrame: DataFrame geoespacial em EPSG:4326.

        Raises:
            FileNotFoundError: Se arquivo não existir.
        """
        if not os.path.isfile(tile_grid_path):
            msg = f"Arquivo Shapefile não encontrado: {tile_grid_path}"
            self.logger.error(msg)
            raise FileNotFoundError(msg)

        try:
            self.logger.info(f"Carregando grade: {tile_grid_path}")
            # Usa fiona para leitura robusta e conversão manual para Shapely
            with fiona.open(tile_grid_path, "r") as collection:
                custom_crs_wkt = collection.crs_wkt
                records = [
                    {"properties": rec["properties"],
                     "geometry": shape(rec["geometry"])}
                    for rec in collection
                ]

            attrs = pd.DataFrame([rec["properties"] for rec in records])
            geoms = gpd.GeoSeries([rec["geometry"] for rec in records],
                                  crs=custom_crs_wkt)
            tile_grid = gpd.GeoDataFrame(attrs, geometry=geoms)
            self.logger.info(
                f"Grade '{os.path.basename(tile_grid_path)}' "
                f"carregada ({len(tile_grid)} tiles)."
            )
        except Exception as e:
            self.logger.error(f"Falha ao carregar grade "
                              f"'{tile_grid_path}': {e}")
            raise

        # Conversão forçada para WGS84 (Lat/Lon)
        if tile_grid.crs and tile_grid.crs.to_epsg() != 4326:
            self.logger.info(f"Convertendo CRS de "
                             f"{tile_grid.crs} para EPSG:4326.")
            tile_grid = tile_grid.to_crs(epsg=4326)

        return tile_grid

    def get_tile_data(
            self, tile_grid: gpd.GeoDataFrame,
            tile_id: str, satellite: str
            ) -> gpd.GeoDataFrame:
        """
        Filtra o GeoDataFrame para encontrar a linha do tile específico.

        Implementa lógicas polimórficas de filtro dependendo do padrão de ID
        de cada satélite (ex: Sentinel-2 usa 'NAME', Landsat usa 'PATH/ROW').

        Args:
            tile_grid (gpd.GeoDataFrame): DataFrame completo.
            tile_id (str): ID buscado.
            satellite (str): Satélite.

        Returns:
            gpd.GeoDataFrame: DataFrame filtrado (deve conter 1 linha).
        """
        if "CB" in satellite.upper() or "S1A" in satellite.upper():
            # CBERS/S1A: IDs como '221_067' viram '221/067'
            normalized_tile_id = tile_id.replace("_", "/")
            return tile_grid[tile_grid["tile"] == normalized_tile_id]
        elif "S2" in satellite.upper():
            # Sentinel-2: ID direto na coluna NAME
            return tile_grid[tile_grid["NAME"] == tile_id]
        else:
            # Landsat: ID '227067' -> Path 227, Row 067
            path = int(tile_id[:3])
            row = int(tile_id[3:])
            return tile_grid[
                (tile_grid["PATH"] == path) & (tile_grid["ROW"] == row)
                ]

    def calculate_tile_bbox(
        self, tile_data: gpd.GeoDataFrame, satellite: str
    ) -> Tuple[List[float], float, float, float]:
        """
        Calcula os metadados espaciais de um tile encontrado.

        Args:
            tile_data (gpd.GeoDataFrame): Linha do tile.
            satellite (str): Satélite.

        Returns:
            Tuple: (BBox Reduzido, Lat Centro, Lon Centro, Raio Aproximado).
        """
        # Garante geometria 2D
        tile_geometry_2d = self.to_2d(tile_data.geometry.iloc[0])
        minx, miny, maxx, maxy = tile_geometry_2d.bounds

        # Aplica redução (exceto CBERS que já tem grade ajustada)
        if "CB" not in satellite:
            main_bbox = self.calculate_reduced_bbox_tile(
                minx, miny, maxx, maxy)
        else:
            main_bbox = [minx, miny, maxx, maxy]

        lat = (miny + maxy) / 2
        lon = (minx + maxx) / 2

        # Estima raio em KM para referência (usando conversão latitude-dependente)
        bbox_width_km = (maxx - minx) * 111.32 * math.cos(math.radians(lat))
        bbox_height_km = (maxy - miny) * 111.32
        radius_km = max(bbox_width_km, bbox_height_km) / 2

        return main_bbox, lat, lon, radius_km

    def calculate_bbox_from_coords(
            self, lat: float,
            lon: float,
            radius_km: float
            ) -> List[float]:
        """
        Delegador para cálculo de BBox a partir de ponto central.
        """
        from .bounding_box_calculator import BoundingBoxCalculator
        self.logger.info("Processando sem tile ID.")
        return BoundingBoxCalculator.calculate(lat, lon, radius_km)

    def extract_bbox_from_footprint(self, item):
        """
        Extrai o BBox real do footprint de um item STAC (Sentinel-1).
        
        Útil porque imagens de Radar não seguem a grade estática perfeitamente.
        """
        footprint = item.properties.get("GeoFootprint")
        if not footprint:
            return None

        try:
            polygon = shape(footprint)
            minx, miny, maxx, maxy = polygon.bounds
            self.logger.info(f"Footprint extraído via Sentinel-1: "
                             f"[{minx}, {miny}, {maxx}, {maxy}]")
            return [minx, miny, maxx, maxy]
        except Exception as e:
            self.logger.error(f"Falha ao extrair bbox do footprint: {e}")
            return None

    def make_tile_id_from_bbox(self, bbox):
        """
        Gera um ID sintético baseado nas coordenadas do BBox.
        
        Usado para nomear arquivos do Sentinel-1 que não possuem Tile ID oficial.
        Formato: 'minx_miny_maxx_maxy' (com 4 casas decimais).
        """
        minx, miny, maxx, maxy = bbox
        return f"{minx:.4f}_{miny:.4f}_{maxx:.4f}_{maxy:.4f}"
