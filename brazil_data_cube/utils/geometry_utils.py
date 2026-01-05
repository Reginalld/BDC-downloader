import logging
from typing import Any

import fiona
import geopandas as gpd
import pandas as pd
from shapely.geometry import shape


class GeometryUtils:
    """
    Validador de qualidade geométrica para seleção de imagens.

    Esta classe é responsável por garantir que uma imagem retornada pelo catálogo STAC
    realmente cobre a área de interesse do usuário de forma significativa.

    Problema Resolvido:
    Muitas vezes, uma imagem de satélite cruza a bounding box do tile apenas na
    borda (ex: 1% de sobreposição). Sem esta validação, o sistema baixaria
    gigabytes de dados inúteis. Esta classe calcula a porcentagem exata de
    interseção para filtrar esses casos.

    Attributes:
        logger (logging.Logger): Logger configurado.
        tile_grid_path (str): Caminho para o Shapefile de referência da grade.
    """
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
        Calcula se a cobertura útil da imagem atende ao requisito mínimo.

        Fluxo de Validação:
        1. Carrega a grade (Shapefile) de referência.
        2. Localiza o polígono exato do Tile (S2, L8 ou CBERS).
        3. Extrai a geometria do Item STAC.
        4. Reprojeção: Converte a geometria do STAC (geralmente WGS84) para
           o mesmo CRS da grade (ex: UTM), permitindo cálculos de área em metros.
        5. Cálculo: (Área Interseção / Área Total do Tile) >= Limite.

        Args:
            item (Any): Item STAC (objeto PySTAC ou dict) contendo geometria.
            tile_id (str): Identificador do tile (ex: 'T22KGA', '227067').
            satellite (str): Identificador do satélite (para regra de busca).
            min_geometry_cover (float): Percentual mínimo (0 a 100).

        Returns:
            bool: True se a imagem cobre área suficiente, False caso contrário.
        """
        try:
            # Usa fiona + geopandas manualmente para evitar erros de driver
            # comuns ao ler shapefiles complexos diretamente com gpd.read_file
            with fiona.open(self.tile_grid_path, 'r') as collection:
                custom_crs_wkt = collection.crs_wkt
                records = [{'properties': rec['properties'],
                            'geometry': shape(rec['geometry'])}
                           for rec in collection]

            attrs = pd.DataFrame([rec['properties'] for rec in records])
            geoms = gpd.GeoSeries([rec['geometry'] for rec in records],
                                  crs=custom_crs_wkt)
            tiles_gdf = gpd.GeoDataFrame(attrs, geometry=geoms)

        except Exception as e:
            self.logger.error(f"Falha ao carregar a grade de tiles"
                              f" '{self.tile_grid_path}': {e}")
            # Se a grade não pode ser lida, não podemos validar a geometria.
            return False
        tile_row = None

        if "S2" in satellite.upper():
            # Sentinel-2 usa campo NAME
            tile_row = tiles_gdf[tiles_gdf["NAME"] == tile_id]

        elif "L8" in satellite.upper():
            # Landsat usa PATH e ROW (ex: "227067")
            path = int(tile_id[:3])
            row = int(tile_id[3:])
            tile_row = tiles_gdf[
                (tiles_gdf["PATH"] == path) & (tiles_gdf["ROW"] == row)
            ]

        elif "CB" in satellite.upper() or "S1A" in satellite.upper():
            tile_row = tiles_gdf[tiles_gdf["tile"] == tile_id]

        if tile_row.empty:
            self.logger.warning(f"Tile {tile_id} não "
                                f"encontrado na grade {satellite}.")
            return False

        # 1. Obtém geometria do tile (já no CRS correto do shapefile)
        tile_geom = tile_row.iloc[0].geometry

        # 2. Obtém geometria da imagem (STAC retorna em Lat/Lon - EPSG:4326)    
        item_geom = shape(item.geometry)

        # 3. Reprojeção Dinâmica:
        # É crucial converter a geometria da imagem para o mesmo sistema de
        # coordenadas do tile (ex: UTM). Calcular área em Lat/Lon (graus) vs
        # Projetada (metros) geraria resultados errados.
        item_geom_reprojected = gpd.GeoSeries(
            [item_geom], crs="EPSG:4326").to_crs(tiles_gdf.crs
                                                 )

        # 4. Cálculo da Interseção
        intersection = tile_geom.intersection(item_geom_reprojected.iloc[0])

        # Normaliza porcentagem (0-100 -> 0.0-1.0)
        percentage_geometry = min_geometry_cover / 100

        # Verifica se a razão da interseção satisfaz o requisito
        if intersection.area / tile_geom.area >= percentage_geometry:
            return True

        self.logger.debug(
            f"Imagem fora do tile {tile_id} - área de interseção insuficiente."
        )
        return False
