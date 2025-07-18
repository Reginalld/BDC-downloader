# brazil_data_cube/tile_processor.py

import time
import logging
import geopandas as gpd
from ..config import TILES_PARANA, SAT_SUPPORTED, LANDSAT_TILES_PARANA
from ..utils.bounding_box_handler import BoundingBoxHandler
from ..utils.logger import ResultManager
from brazil_data_cube.processors.image_processor import ImageProcessor
from brazil_data_cube.downloader.download_bands import DownloadBands
import os
from brazil_data_cube.minio.MinioUploader import MinioUploader
from typing import List, Dict, Any

class TileProcessor:

    def __init__(self, logger: logging.Logger, fetcher: any, downloader: any, output_dir: str,
                 tile_grid_path: str, max_cloud_cover: float, minio_uploader: any):
        self.fetcher = fetcher
        self.logger = logger
        self.downloader = downloader
        self.output_dir = output_dir
        self.tile_grid_path = tile_grid_path
        self.max_cloud_cover = max_cloud_cover
        self.bbox_handler = BoundingBoxHandler(self.logger)
        self.result_manager = ResultManager(logger)
        # self.image_processor = ImageProcessor(satellite="")  # Será redefinido na execução
        self.minio_uploader = minio_uploader

    def process_parana_tiles(self, satellite: str, start_date: str, end_date: str) -> None:
        """
        Processa todos os tiles do Paraná, baixa e monta o mosaico final.
        
        Args:
            satelite (str): Nome do satélite
            start_date (str): Data de início (YYYY-MM-DD)
            end_date (str): Data final (YYYY-MM-DD)
        """


        if satellite not in SAT_SUPPORTED:
            self.logger.error(f"Satélite '{satellite}' não é suportado.")
            self.result_manager.log_error_csv("Paraná", satellite, "Satélite não suportado")
            return

        tile_mosaic_files = []
        results_time_estimated = []

        if satellite == "S2_L2A-1":
            tile_list = TILES_PARANA
        else:
            tile_list = LANDSAT_TILES_PARANA

        # Itera sobre cada tile definido para o estado do Paraná
        for tile in tile_list:
            logging.info(tile)
            start = time.perf_counter()
            self.logger.info(f"Processando tile {tile}...")

            # Carrega o shapefile da grade de tiles e filtra pelo nome do tile
            tile_grid = gpd.read_file(self.tile_grid_path)
            
            if satellite == "S2_L2A-1":
                tile_grid = tile_grid[tile_grid["NAME"] == tile]
            else:
                path = int(tile[:3])
                row = int(tile[3:])
                tile_grid = tile_grid[(tile_grid["PATH"] == path) & (tile_grid["ROW"] == row)]

            if tile_grid.empty:
                self.logger.warning(f"Tile {tile} não encontrado na grade Sentinel-2. Pulando...")
                continue

            # Calcula o bounding box reduzido para evitar bordas e melhorar qualidade das imagens
            main_bbox = self.bbox_handler.calculate_reduced_bbox(tile_grid)

            # Busca as imagens disponíveis que atendem aos critérios
            image_assets = self.fetcher.fetch_image(
                satellite, main_bbox, start_date, end_date,
                self.max_cloud_cover, self.tile_grid_path, tile
            )

            if not image_assets:
                self.logger.warning(f"Nenhuma imagem encontrada para o tile {tile}.")
                continue

            # Prefixo usado nos nomes dos arquivos baixados e processados
            prefix = f"{tile}_{satellite}_{start_date}_{end_date}"
                    
            self.logger.info("Baixando e processando imagens...")
            downloaded_files = DownloadBands(self.logger).download_bands(image_assets, self.downloader, prefix, satellite, self.minio_uploader, tile)

            # Caminho do arquivo final (mosaico) para o tile atual
            tile_mosaic_output = os.path.join(self.output_dir, f"{satellite}_{tile}_{start_date}_{end_date}_RGB.tif")

            # (Comentado) Processamento para gerar o mosaico final a partir das bandas baixadas
            # Pode ser ativado no futuro quando a montagem do mosaico estiver implementada
            # self.image_processor.merge_bandas_tif(...)

            data_range_folder = f"{start_date}_{end_date}"

            for path in downloaded_files.values():
                self.minio_uploader.upload_file(path, object_name=os.path.join(satellite, tile or 'ponto', os.path.basename(path)))

            tile_mosaic_files.append(tile_mosaic_output)
            duration = time.perf_counter() - start
            results_time_estimated.append({"Tile_id": tile, "duration_sec": duration})

        
        # Gera o relatório final de resultados e tempo de execução por tile
        self.result_manager.manage_results(
            tile_mosaic_files, results_time_estimated,satellite,start_date
        )