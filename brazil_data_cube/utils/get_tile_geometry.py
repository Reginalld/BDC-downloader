import logging

import fiona
import geopandas as gpd
import pandas as pd
from shapely.geometry import shape


class GeometryLoader:
    """
    Carregador de geometrias de referência a partir de arquivos Shapefile.

    Esta classe abstrai a leitura de grades espaciais (Grids) de diferentes missões.
    Seu principal objetivo é recuperar o polígono exato de um Tile específico
    para garantir que a gravação no Banco de Dados tenha precisão geográfica,
    em vez de usar apenas um Bounding Box retangular aproximado.

    Attributes:
        logger (logging.Logger): Logger configurado.
        tile_grid_path (str): Caminho absoluto do arquivo .shp.
    """
    def __init__(self, logger: logging.Logger, tile_grid_path: str):
        self.logger = logger
        self.tile_grid_path = tile_grid_path

    def get_tile_geometry(self, tile_id: str, satellite: str):
        """
        Recupera a geometria vetorial (Polígono) de um tile específico.

        O método implementa lógica polimórfica para lidar com as diferentes
        convenções de nomenclatura de colunas nos Shapefiles do INPE:
        - Sentinel-2: Busca pela coluna 'NAME'.
        - Landsat-8: Faz o parse do ID (ex: '227067') em 'PATH' e 'ROW'.
        - CBERS/S1A: Busca pela coluna 'tile'.

        Além disso, realiza a reprojeção automática para EPSG:4326 (Lat/Lon),
        que é o padrão exigido pelo PostGIS no banco de dados.

        Args:
            tile_id (str): Identificador do tile (ex: 'T22KGA', '227067').
            satellite (str): Identificador do satélite (ex: 'S2A', 'L8').

        Returns:
            Optional[Polygon]: Objeto geométrico Shapely em WGS84 ou None se não encontrar.
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
            self.logger.error(
                f"Erro ao carregar grade {self.tile_grid_path}: {e}")
            return None

        # Conversão manual para GeoDataFrame (mais seguro que gpd.read_file em alguns envs)
        attrs = pd.DataFrame([rec['properties'] for rec in records])
        geoms = gpd.GeoSeries(
            [rec['geometry'] for rec in records], crs=custom_crs)
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
                tile_row = tiles_gdf[
                    (tiles_gdf["PATH"] == path) & (tiles_gdf["ROW"] == row)]
            except ValueError:
                self.logger.error(f"Tile Landsat inválido: {tile_id}")
                return None

        elif "CB" in sat.upper() or "S1A" in sat:
            # Grades BDC (CBERS) e Sentinel-1 usam coluna 'tile'
            # Nota: O ID esperado aqui já deve estar normalizado (ex: '221/067' ou '221_067'
            # dependendo de como foi passado, mas geralmente o shapefile usa '/')
            # O ideal é garantir que o caller normalizou, ou tentar ambas as formas aqui.
            tile_row = tiles_gdf[tiles_gdf["tile"] == tile_id]

        if tile_row is None or tile_row.empty:
            self.logger.warning(f"Tile {tile_id} não encontrado no SHP.")
            return None

        try:
            # O Banco de Dados espera EPSG:4326 (Lat/Lon).
            # Se o shapefile estiver em UTM, transformamos agora.
            transformed_row = tile_row.to_crs("EPSG:4326")
            return transformed_row.iloc[0].geometry
        except Exception as e:
            self.logger.error(
                f"Erro na transformação de CRS para {tile_id}: {e}")
            # Fallback: Retorna a geometria original (melhor ter algo que nada,
            # mas o PostGIS pode reclamar de SRID misto).
            return tile_row.iloc[0].geometry
