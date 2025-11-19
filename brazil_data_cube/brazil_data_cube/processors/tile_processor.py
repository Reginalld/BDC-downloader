import logging
import os
import time
from datetime import datetime

import fiona
import geopandas as gpd
from shapely.geometry import shape

from brazil_data_cube.downloader.download_bands import DownloadBands

from ..utils.bounding_box_handler import BoundingBoxHandler
from ..utils.logger import ResultManager


class TileProcessor:
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

    def resolve_tile(self, grid_master, tile, satellite):
        """Resolve grade + mission info."""
        sat = satellite.upper()

        if "S2" in sat:
            grid = grid_master[grid_master["NAME"] == tile]
            return grid, "s2", "SENTINEL2", "S2A", "L2A"

        if "CB" in sat:
            normalized = tile.replace("_", "/")
            grid = grid_master[grid_master["tile"] == normalized]
            return grid, "cbers", "CBERS", "CB4", "SR"

        if "S1A" in sat:
            normalized = tile.replace("_", "/")
            grid = grid_master[grid_master["tile"] == normalized]
            return grid, "s1", "SENTINEL1", "S1A", "SAR"

        if "L8" in sat:
            path, row = int(tile[:3]), int(tile[3:])
            grid = grid_master[
                (grid_master["PATH"] == path) &
                (grid_master["ROW"] == row)
            ]
            return grid, "landsat", "LANDSAT", "L8", "LEVEL2"

        return gpd.GeoDataFrame(), None, None, None, None

    def build_prefix(self, sat, mission, tile, data_criacao, level):
        """Monta prefixo para nome do arquivo."""
        if data_criacao:
            dt = datetime.fromisoformat(data_criacao.replace("Z", "+00:00"))
            data_formatada = dt.strftime("%Y%m%d")
        else:
            data_formatada = "00000000"
        return f"{sat}_{mission}_{tile}_{data_formatada}_{level}"
    

    def process_single_tile(self, tile, grid_master, satellite, start_date, end_date):

        tile_grid, minio_prefix, mission, sat, level = \
            self.resolve_tile(grid_master, tile, satellite)

        if tile_grid.empty:
            self.logger.warning(f"Tile {tile} não encontrada.")
            return None

        # Bounding box
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
            sat, mission, tile,
            assets.properties.get("start_datetime", ""),
            level
        )

        # Download
        downloaded = DownloadBands(self.logger).download_bands(
            assets.assets,
            self.downloader,
            prefix,
            satellite,
            self.minio_uploader,
            minio_prefix
        )

        # Upload
        self.minio_uploader.upload_and_cleanup_batch(downloaded, minio_prefix)

        # Output mosaic path
        mosaic_path = os.path.join(
            self.output_dir,
            f"{satellite}_{tile}_{start_date}_{end_date}_RGB.tif"
        )
        return mosaic_path

    def process_tile_list(self, tiles, satellite, start_date, end_date):

        grid_master = self.bbox_handler.load_tile_grid(self.tile_grid_path)
        if grid_master is None:
            return

        mosaic_files = []
        durations = []

        for tile in tiles:
            self.logger.info(f"Processando tile {tile}...")
            start = time.perf_counter()

            try:
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
