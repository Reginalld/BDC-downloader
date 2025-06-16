# brazil_data_cube/tile_processor.py

import time
import logging
import geopandas as gpd
from ..config import TILES_PARANA, SAT_SUPPORTED
from ..utils.bounding_box_handler import BoundingBoxHandler
from ..utils.logger import ResultManager
from brazil_data_cube.processors.image_processor import ImageProcessor
from brazil_data_cube.downloader.download_bandas import DownloadBandas
import os
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
        logging.info(TILES_PARANA)
        for tile in TILES_PARANA:
            logging.info(tile)
            start = time.perf_counter()
            logger.info(f"Processando tile {tile}...")

            tile_grid = gpd.read_file(self.tile_grid_path)
            tile_grid = tile_grid[tile_grid["NAME"] == tile]

            if tile_grid.empty:
                logger.warning(f"Tile {tile} não encontrado na grade Sentinel-2. Pulando...")
                continue

            main_bbox = self.bbox_handler.calcular_bbox_reduzido(tile_grid)

            image_assets = self.fetcher.fetch_image(
                satelite, main_bbox, start_date, end_date,
                self.max_cloud_cover, self.tile_grid_path, tile
            )

            if not image_assets:
                logger.warning(f"Nenhuma imagem encontrada para o tile {tile}.")
                continue

            prefixo = (
                        f"{tile}_{satelite}_{start_date}_{end_date}"
                    )
            logger.info("Baixando e processando imagens...")
            DownloadBandas.baixar_bandas(image_assets,self.downloader,prefixo,satelite)


            tile_mosaic_output = os.path.join(self.output_dir, f"{satelite}_{tile}_{start_date}_{end_date}_RGB.tif")
            # ImageProcessor(satelite).merge_bandas_tif(
            #     r, g, b, b08, aot, b01, b05, b06,
            #     b07, b09, b11, b12, b8a, pvi, scl,
            #     tci, wvp, tile_mosaic_output
            # )

            tile_mosaic_files.append(tile_mosaic_output)
            duration = time.perf_counter() - start
            results_time_estimated.append({"Tile_id": tile, "duration_sec": duration})

        self.result_manager.gerenciar_resultados(
            tile_mosaic_files, results_time_estimated
        )