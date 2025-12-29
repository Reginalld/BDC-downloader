import logging
import os
import time
from datetime import datetime
from typing import Optional, List

import geopandas as gpd

from brazil_data_cube.downloader.download_bands import DownloadBands
from brazil_data_cube.utils.mission_info import MissionInfo

from ..utils.bounding_box_handler import BoundingBoxHandler
from ..utils.logger import ResultManager


class TileProcessor:
    """
    Processador de lote otimizado para ingestão massiva de tiles.

    Diferente do processamento unitário, esta classe é projetada para iterar
    listas extensas (ex: todos os tiles do Estado), otimizando o I/O de disco
    ao carregar a malha de tiles (Grid Master) apenas uma vez na memória.

    Attributes:
        logger (logging.Logger): Logger configurado.
        fetcher (SatelliteImageFetcher): Cliente de busca STAC.
        downloader (ImageDownloader): Referência ao orquestrador principal.
        output_dir (str): Diretório base para outputs.
        tile_grid_path (str): Caminho do shapefile da grade.
        max_cloud_cover (float): Limite de nuvens.
        minio_uploader (MinioUploader): Cliente de upload.
        min_geometry_cover (float): Limite de geometria.
        bbox_handler (BoundingBoxHandler): Utilitário de geometria.
        result_manager (ResultManager): Gerenciador de métricas e logs.
    """
    def __init__(
        self,
        logger: logging.Logger,
        fetcher: any,
        downloader: any,
        output_dir: str,
        tile_grid_path: str,
        max_cloud_cover: float,
        minio_uploader: any,
        min_geometry_cover: float
    ):
        self.fetcher = fetcher
        self.logger = logger
        self.downloader = downloader
        self.output_dir = output_dir
        self.tile_grid_path = tile_grid_path
        self.max_cloud_cover = max_cloud_cover
        self.bbox_handler = BoundingBoxHandler(self.logger)
        self.result_manager = ResultManager(logger)
        self.minio_uploader = minio_uploader
        self.min_geometry_cover = min_geometry_cover

    def build_prefix(
        self,
        sat: str,
        mission: str,
        tile: str,
        data_criacao: str,
        level: str
         ) -> str:
        """
        Constrói o nome canônico do arquivo (Prefixo).

        Padroniza: SAT_MISSAO_TILE_DATA_NIVEL.

        Args:
            sat (str): Nome curto (ex: S2).
            mission (str): Nome da missão (ex: SENTINEL2).
            tile (str): ID do tile.
            data_criacao (str): Data ISO 8601.
            level (str): Nível de processamento (ex: L2A).

        Returns:
            str: Prefixo formatado (ex: S2_SENTINEL2_T22KGA_20230615_L2A).
        """
        if data_criacao:
            dt = datetime.fromisoformat(data_criacao.replace("Z", "+00:00"))
            data_formatada = dt.strftime("%Y%m%d")
        else:
            data_formatada = "00000000"
        return f"{sat}_{mission}_{tile}_{data_formatada}_{level}"

    def process_single_tile(
            self,
            tile: str,
            grid_master: gpd.GeoDataFrame,
            satellite: str,
            start_date: str,
            end_date: str) -> Optional[str]:
        """
        Executa o pipeline de ingestão para um único tile dentro do lote.

        1. Filtro de Memória: Busca a geometria do tile no `grid_master` pré-carregado. # noqa: E501
        2. Cálculo de BBox: Reduz o polígono para evitar overlaps.
        3. Busca: Consulta o STAC.
        4. Download e Upload: Baixa e persiste os arquivos.

        Args:
            tile (str): ID do Tile (ex: 'T22KGA' ou '227067').
            grid_master (gpd.GeoDataFrame): O Shapefile completo carregado na RAM.
            satellite (str): Identificador do satélite.
            start_date (str): Data inicial.
            end_date (str): Data final.

        Returns:
            Optional[str]: Caminho do mosaico gerado (ou None em caso de falha/skip).
        """

        mission_info = MissionInfo(satellite)

        sat_upper = satellite.upper()
        tile_grid = gpd.GeoDataFrame()

        # --- Lógica de Filtragem no DataFrame (Pandas/GeoPandas) ---
        # Como o grid_master contém TODOS os tiles do Brasil, filtramos
        # apenas a linha que corresponde ao ID atual. A coluna varia por satélite. # noqa: E501
        if "S2" in sat_upper:
            # Sentinel-2 usa a coluna 'NAME' (MGRS)
            tile_grid = grid_master[grid_master["NAME"] == tile]
        elif "CB" in sat_upper:
            # CBERS e S1A usam 'tile' com separador (ex: 221/067 -> 221_067)
            normalized = tile.replace("_", "/")
            tile_grid = grid_master[grid_master["tile"] == normalized]
        elif "S1A" in sat_upper:
            normalized = tile.replace("_", "/")
            tile_grid = grid_master[grid_master["tile"] == normalized]
        elif "L8" in sat_upper:
            # Landsat usa PATH e ROW inteiros
            try:
                path, row = int(tile[:3]), int(tile[3:])
                tile_grid = grid_master[
                    (grid_master["PATH"] == path) & (grid_master["ROW"] == row)
                ]
            except ValueError:
                self.logger.error(f"Tile Landsat inválido: {tile}")
                return None

        if tile_grid.empty:
            self.logger.warning(f"Tile {tile} não encontrada.")
            return None

        # Converte para WGS84 e calcula BBox com margem de segurança
        tile_grid_wgs84 = tile_grid.to_crs("EPSG:4326")
        bbox = self.bbox_handler.calculate_reduced_bbox(tile_grid_wgs84)

        # Busca imagem
        assets = self.fetcher.fetch_image(
            satellite, bbox, start_date, end_date,
            self.max_cloud_cover, self.tile_grid_path,
            self.min_geometry_cover, tile
        )
        if not assets:
            self.logger.warning(f"Nenhuma imagem encontrada para {tile}.")
            return None

        prefix = self.build_prefix(
            mission_info.sat, mission_info.mission, tile,
            assets.properties.get("start_datetime", ""),
            mission_info.level
        )

        # Download
        downloaded = DownloadBands(self.logger).download_bands(
            assets.assets,
            self.downloader.http_downloader,
            prefix,
            satellite,
            self.minio_uploader,
            mission_info.bucket_prefix,
            self.output_dir
        )

        # Upload
        self.downloader.scene_persister.persist_batch(
            download_files=downloaded,
            mission_info=mission_info,
            tile_id=tile,
            start_datetime_str=assets.properties.get("start_datetime", ""),
            bbox=bbox
        )

        mosaic_path = os.path.join(
            self.output_dir,
            f"{satellite}_{tile}_{start_date}_{end_date}_RGB.tif"
        )
        return mosaic_path

    def process_tile_list(
        self,
        tiles: List[str],
        satellite: str,
        start_date: str,
        end_date: str
    ) -> None:
        """
        Ponto de entrada para processamento em lote.

        Otimização: Carrega o arquivo Shapefile de grade (que pode ser grande)
        uma única vez no início do processo e passa a referência em memória
        para as iterações subsequentes.

        Args:
            tiles (List[str]): Lista de IDs de tiles para processar.
            satellite (str): Identificador do satélite.
            start_date (str): Data inicial.
            end_date (str): Data final.
        """

        grid_master = self.bbox_handler.load_tile_grid(self.tile_grid_path)
        if grid_master is None:
            return

        mosaic_files = []
        durations = []

        for tile in tiles:
            self.logger.info(f"Processando tile {tile}...")
            start = time.perf_counter()

            try:
                print(satellite)
                mosaic = self.process_single_tile(
                    tile, grid_master, satellite, start_date, end_date)

                if mosaic:
                    mosaic_files.append(mosaic)

            except Exception as e:
                self.logger.error(f"Erro ao processar tile {tile}: {e}")

            duration = time.perf_counter() - start
            durations.append({"Tile_id": tile, "duration_sec": duration})

        self.result_manager.manage_results(
            mosaic_files, durations, satellite, start_date
        )
