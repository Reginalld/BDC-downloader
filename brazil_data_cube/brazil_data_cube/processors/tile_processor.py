import logging
import os
import time
from datetime import datetime

import fiona
import geopandas as gpd
from shapely.geometry import shape

from brazil_data_cube.downloader.download_bands import DownloadBands

from ..config import SAT_SUPPORTED
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

    def _load_grid_robustly(self):
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

    def process_tile_list(
        self,
        tiles_list: any,
        satellite: str,
        start_date: str,
        end_date: str
    ) -> None:
        """
        Processa todos os tiles do Paraná, baixa e monta o mosaico final.
        """
        if satellite not in SAT_SUPPORTED:
            self.logger.error(f"Satélite '{satellite}' não é suportado.")
            self.result_manager.log_error_csv(
                "Paraná", satellite, "Satélite não suportado"
            )
            return

        tile_grid_master = self._load_grid_robustly()
        if tile_grid_master is None:
            self.logger.error("Não foi possível carregar a grade "
                              "de tiles. Abortando processo.")
            return

        tile_mosaic_files = []
        results_time_estimated = []
        tile_list = tiles_list
        caminho_minio = None

        for tile in tile_list:
            logging.info(tile)
            start = time.perf_counter()
            self.logger.info(f"Processando tile {tile}...")

            if satellite == "S2":
                tile_grid = tile_grid_master[tile_grid_master["NAME"] == tile]
                caminho_minio = "s2"
                mission = "SENTINEL2"
                sat = "S2A"
                level = "L2A"
            elif "CB" in satellite:
                normalized_tile_id = tile.replace("_", "/")
                tile_grid = tile_grid_master[
                    tile_grid_master["tile"] == normalized_tile_id
                    ]
                caminho_minio = "cbers"
                mission = "CBERS"
                sat = "CB4"
                level = "SR"
            else:
                path = int(tile[:3])
                row = int(tile[3:])
                tile_grid = tile_grid_master[
                    (tile_grid_master["PATH"] == path)
                    & (tile_grid_master["ROW"] == row)
                ]
                mission = "LANDSAT"
                sat = "L9"
                level = "LEVEL2"
                caminho_minio = "landsat"

            if tile_grid.empty:
                self.logger.warning(
                    f"Tile {tile} não encontrado "
                    "na grade Sentinel-2. Pulando..."
                )
                continue

            self.logger.info("Reprojetando a geometria do tile para "
                             "EPSG:4326 antes de calcular o bbox da API...")
            tile_grid_wgs84 = tile_grid.to_crs("EPSG:4326")

            main_bbox = self.bbox_handler.calculate_reduced_bbox(
                tile_grid_wgs84)

            image_assets = self.fetcher.fetch_image(
                satellite,
                main_bbox,
                start_date,
                end_date,
                self.max_cloud_cover,
                self.tile_grid_path,
                self.min_geometry_cover,
                tile
            )

            if not image_assets:
                self.logger.warning(
                    f"Nenhuma imagem encontrada para o tile {tile}."
                )
                continue

            data_criacao = image_assets.properties.get('created', '')

            if data_criacao:
                # Converte de ISO para datetime
                dt = datetime.fromisoformat(
                    data_criacao.replace("Z", "+00:00"))
                # Formata para o padrão YYYYMMDD
                data_formatada = dt.strftime("%Y%m%d")
            else:
                data_formatada = "00000000"  # fallback

            prefix = (
                    f"{sat}_{mission}_{tile}"
                    f"_{data_formatada}_{level}"
                )
            image_assets = image_assets.assets

            self.logger.info("Baixando e processando imagens...")
            downloaded_files = DownloadBands(self.logger).download_bands(
                image_assets,
                self.downloader,
                prefix,
                satellite,
                self.minio_uploader,
                caminho_minio,
                tile
            )

            tile_mosaic_output = os.path.join(
                self.output_dir,
                f"{satellite}_{tile}_{start_date}_{end_date}_RGB.tif"
            )

            for path in downloaded_files.values():

                object_name = os.path.join(
                    caminho_minio,
                    os.path.basename(path)
                )

                self.minio_uploader.upload_file(
                    path,
                    object_name=object_name
                )

                if self.minio_uploader.object_exists(object_name, x=1):
                    self.remover_loger.info(
                        f"Arquivo no diretório {path} será deletado localmente"
                        )
                    try:
                        os.remove(path)
                        self.remover_loger.info(
                            f"Arquivo {path} deletado com sucesso."
                            )
                    except FileNotFoundError:
                        self.remover_loger.warning(
                            f"Arquivo {path} não encontrado para deletar."
                            )
                    except Exception as e:
                        self.remover_loger.error(
                            f"Erro ao deletar o arquivo {path}: {e}")

            tile_mosaic_files.append(tile_mosaic_output)
            duration = time.perf_counter() - start
            results_time_estimated.append(
                {"Tile_id": tile, "duration_sec": duration}
            )

        self.result_manager.manage_results(
            tile_mosaic_files,
            results_time_estimated,
            satellite,
            start_date
        )
