# brazil_data_cube/tile_processor.py

import time
import logging
import geopandas as gpd
from ..config import TILES_PARANA, SAT_SUPPORTED, LANDSAT_TILES_PARANA
from ..utils.bounding_box_handler import BoundingBoxHandler
from ..utils.logger import ResultManager
from brazil_data_cube.processors.image_processor import ImageProcessor
from brazil_data_cube.downloader.download_bandas import DownloadBandas
import os
from brazil_data_cube.minio.MinioUploader import MinioUploader
from typing import List, Dict, Any

logger = logging.getLogger(__name__)

class TileProcessor:

    def __init__(self, fetcher: any, downloader: any, output_dir: str,
                 tile_grid_path: str, max_cloud_cover: float):
        self.fetcher = fetcher
        self.downloader = downloader
        self.output_dir = output_dir
        self.tile_grid_path = tile_grid_path
        self.max_cloud_cover = max_cloud_cover
        self.bbox_handler = BoundingBoxHandler()
        self.result_manager = ResultManager()
        self.image_processor = ImageProcessor(satelite="")  # Será redefinido na execução
        self.minio_uploader = MinioUploader(
            endpoint="localhost:9000",
            access_key="P8qQeeRKP6pHWDGuKiLi",
            secret_key="v7aKWRVPoN76hNQirzefTeeWsnSsNGHlz5AHI1QU",
            secure=False,
            bucket_name= "imagens-brutas"
        )


    def processar_tiles_parana(self, satelite: str, start_date: str, end_date: str) -> None:
        """
        Processa todos os tiles do Paraná, baixa e monta o mosaico final.
        
        Args:
            satelite (str): Nome do satélite
            start_date (str): Data de início (YYYY-MM-DD)
            end_date (str): Data final (YYYY-MM-DD)
        """


        if satelite not in SAT_SUPPORTED:
            logger.error(f"Satélite '{satelite}' não é suportado.")
            self.result_manager.log_error_csv("Paraná", satelite, "Satélite não suportado")
            return

        tile_mosaic_files = []
        results_time_estimated = []

        if satelite == "S2_L2A-1":
            tile_list = TILES_PARANA
        else:
            tile_list = LANDSAT_TILES_PARANA

        # Itera sobre cada tile definido para o estado do Paraná
        for tile in tile_list:
            logging.info(tile)
            start = time.perf_counter()
            logger.info(f"Processando tile {tile}...")

            # Carrega o shapefile da grade de tiles e filtra pelo nome do tile
            tile_grid = gpd.read_file(self.tile_grid_path)
            
            if satelite == "S2_L2A-1":
                tile_grid = tile_grid[tile_grid["NAME"] == tile]
            else:
                path = int(tile[:3])
                row = int(tile[3:])
                tile_grid = tile_grid[(tile_grid["PATH"] == path) & (tile_grid["ROW"] == row)]

            if tile_grid.empty:
                logger.warning(f"Tile {tile} não encontrado na grade Sentinel-2. Pulando...")
                continue

            # Calcula o bounding box reduzido para evitar bordas e melhorar qualidade das imagens
            main_bbox = self.bbox_handler.calcular_bbox_reduzido(tile_grid)

            # Busca as imagens disponíveis que atendem aos critérios
            image_assets = self.fetcher.fetch_image(
                satelite, main_bbox, start_date, end_date,
                self.max_cloud_cover, self.tile_grid_path, tile
            )

            if not image_assets:
                logger.warning(f"Nenhuma imagem encontrada para o tile {tile}.")
                continue

            # Prefixo usado nos nomes dos arquivos baixados e processados
            prefixo = f"{tile}_{satelite}_{start_date}_{end_date}"
                    
            logger.info("Baixando e processando imagens...")
            arquivos_baixados = DownloadBandas.baixar_bandas(image_assets,self.downloader,prefixo,satelite)

            # Caminho do arquivo final (mosaico) para o tile atual
            tile_mosaic_output = os.path.join(self.output_dir, f"{satelite}_{tile}_{start_date}_{end_date}_RGB.tif")

            # (Comentado) Processamento para gerar o mosaico final a partir das bandas baixadas
            # Pode ser ativado no futuro quando a montagem do mosaico estiver implementada
            # self.image_processor.merge_bandas_tif(...)

            data_range_folder = f"{start_date}_{end_date}"

            for path in arquivos_baixados.values():
                self.minio_uploader.upload_file(path, object_name=os.path.join(satelite, data_range_folder, tile or 'ponto', os.path.basename(path)))

            tile_mosaic_files.append(tile_mosaic_output)
            duration = time.perf_counter() - start
            results_time_estimated.append({"Tile_id": tile, "duration_sec": duration})

        
        # Gera o relatório final de resultados e tempo de execução por tile
        self.result_manager.gerenciar_resultados(
            tile_mosaic_files, results_time_estimated,satelite,start_date
        )