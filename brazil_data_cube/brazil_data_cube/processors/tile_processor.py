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
        remover_loger: logging.Logger,
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
        self.remover_loger = remover_loger
        self.downloader = downloader
        self.output_dir = output_dir
        self.tile_grid_path = tile_grid_path
        self.max_cloud_cover = max_cloud_cover
        self.bbox_handler = BoundingBoxHandler(self.logger)
        self.result_manager = ResultManager(logger)
        self.minio_uploader = minio_uploader
        self.min_geometry_cover = min_geometry_cover

    def load_grid_robustly(self):
        """
        Carrega a grade de tiles de forma robusta,
        lidando com projeções customizadas
        que causam erro no gpd.read_file() padrão.
        """
        self.logger.info(f"Carregando grade com "
                         f"projeção customizada: {self.tile_grid_path}")
        try:
            with fiona.open(self.tile_grid_path, 'r') as collection:
                custom_crs_wkt = collection.crs_wkt
                records = [
                    {'properties': rec['properties'],
                     'geometry': shape(rec['geometry'])} for rec in collection
                    ]

            attrs = gpd.pd.DataFrame([rec['properties'] for rec in records])
            geoms = gpd.GeoSeries([rec['geometry'] for rec in records],
                                  crs=custom_crs_wkt)
            grid = gpd.GeoDataFrame(attrs, geometry=geoms)

            self.logger.info(f"Grade '{os.path.basename(self.tile_grid_path)}'"
                             f" carregada com sucesso ({len(grid)} tiles).")
            return grid
        except Exception as e:
            self.logger.error(f"Falha crítica ao "
                              f"carregar a grade de tiles: {e}")
            return None

    def select_tile_grid(self, tile_grid_master, tile, satellite):
        """Seleciona o tile correspondente no shapefile"""
        if "S2" in satellite.upper():
            return tile_grid_master[
                tile_grid_master["NAME"] == tile
                ], "s2", "SENTINEL2", "S2A", "L2A"

        elif "CB" in satellite.upper():
            normalized_tile_id = tile.replace("_", "/")
            grid = tile_grid_master[
                tile_grid_master["tile"] == normalized_tile_id]
            return grid, "cbers", "CBERS", "CB4", "SR"

        elif "L8" in satellite.upper():
            path, row = int(tile[:3]), int(tile[3:])
            grid = tile_grid_master[
                (tile_grid_master["PATH"] == path)
                & (tile_grid_master["ROW"] == row)
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

    def upload_and_cleanup(self, downloaded_files, minio_prefix):
        """Faz upload para o MinIO e remove arquivos locais."""
        for path in downloaded_files.values():
            object_name = os.path.join(minio_prefix, os.path.basename(path))
            self.minio_uploader.upload_file(path, object_name=object_name)

            if self.minio_uploader.object_exists(object_name, x=1):
                self.remover_loger.info(f"Removendo {path} do disco local")
                try:
                    os.remove(path)
                except FileNotFoundError:
                    self.remover_loger.warning(
                        f"{path} não encontrado para deletar.")
                except Exception as e:
                    self.remover_loger.error(f"Erro ao deletar {path}: {e}")

    def process_tile_list(self, tiles_list, satellite, start_date, end_date):
        """Processa todos os tiles fornecidos."""
        tile_grid_master = self.load_grid_robustly()
        if tile_grid_master is None:
            self.logger.error("Abortande: grade de tiles não carregada.")
            return

        results_time_estimated = []
        tile_mosaic_files = []

        for tile in tiles_list:
            start = time.perf_counter()
            self.logger.info(f"Processando tile {tile}...")

            # Seleciona grade e infos da missão
            tile_grid, minio_prefix, mission, sat, level = self. \
                select_tile_grid(tile_grid_master, tile, satellite)

            if tile_grid.empty:
                self.logger.warning(f"Tile {tile} não encontrada. Pulando...")
                continue

            # Converte CRS antes de calcular bbox
            tile_grid_wgs84 = tile_grid.to_crs("EPSG:4326")
            main_bbox = self.bbox_handler.calculate_reduced_bbox(
                tile_grid_wgs84)

            # Busca imagens
            image_assets = self.fetcher.fetch_image(
                satellite,
                main_bbox,
                start_date,
                end_date,
                self.max_cloud_cover,
                self.tile_grid_path,
                self.min_geometry_cover,
                tile,
            )
            if not image_assets:
                self.logger.warning(f"Nenhuma imagem encontrada para {tile}.")
                continue

            # Monta prefixo com data
            prefix = self.build_prefix(
                sat,
                mission,
                tile,
                image_assets.properties.get("created", ""),
                level
            )

            # Download + upload
            self.logger.info("Baixando imagens...")
            downloaded_files = DownloadBands(self.logger).download_bands(
                image_assets.assets,
                self.downloader,
                prefix,
                satellite,
                self.minio_uploader,
                minio_prefix
            )
            self.upload_and_cleanup(downloaded_files, minio_prefix)

            # Prepara resultados
            tile_mosaic_output = os.path.join(
                self.output_dir,
                f"{satellite}_{tile}_{start_date}_{end_date}_RGB.tif"
            )
            tile_mosaic_files.append(tile_mosaic_output)

            duration = time.perf_counter() - start
            results_time_estimated.append(
                {"Tile_id": tile, "duration_sec": duration})

        # Finaliza
        self.result_manager.manage_results(
            tile_mosaic_files, results_time_estimated, satellite, start_date
        )
