# brazil_data_cube/downloader/image_downloader.py

import json
import logging
import os
import time
from datetime import datetime
from typing import Optional

import requests
from tqdm import tqdm

from brazil_data_cube.config import (REDUCTION_FACTOR, SHAPEFILE_PATH_LANDSAT,
                                     TILES_PATH_LANDSAT, TILES_PATH_SENTINEL)
from brazil_data_cube.downloader.download_bands import DownloadBands
from brazil_data_cube.downloader.fetcher import SatelliteImageFetcher
from brazil_data_cube.minio.MinioUploader import MinioUploader
from brazil_data_cube.processors.tile_processor import TileProcessor
from brazil_data_cube.utils.bdc_connection import BdcConnection
from brazil_data_cube.utils.bounding_box_handler import BoundingBoxHandler

with open(TILES_PATH_LANDSAT, "r", encoding="utf-8") as f:
    LANDSAT_TILES_POR_UF = json.load(f)

with open(TILES_PATH_SENTINEL, "r", encoding="utf-8") as f:
    SENTINEL_TILES_POR_UF = json.load(f)


class ImageDownloader:
    def __init__(self, logger: logging.Logger, output_dir: str):
        self.output_dir = output_dir
        self.logger = logger
        self.create_output()

    def create_output(self) -> None:
        """Cria diretório de saída se ele não existir."""
        os.makedirs(self.output_dir, exist_ok=True)
        self.logger.info(f"Diretório de saída criado em: {self.output_dir}")

    def download(
        self,
        asset: dict,
        filename: str,
        request_options: dict = {}
    ) -> Optional[str]:
        """
        Baixa um asset usando requisição HTTP.
        """
        if asset is None:
            self.logger.error("Tentativa de download com asset inválido.")
            return None

        filepath = os.path.join(self.output_dir, filename)
        self.logger.info(f"Iniciando download da imagem para: {filepath}")

        max_retries = 3
        backoff_factor = 2.0
        attempt = 0
        while attempt < max_retries:
            try:
                response = requests.get(
                    asset.href,
                    stream=True,
                    timeout=30,
                    **request_options
                )
                response.raise_for_status()

                total_bytes = int(response.headers.get('content-length', 0))
                chunk_size = 1024 * 16

                with tqdm.wrapattr(
                    open(filepath, 'wb'),
                    'write',
                    miniters=1,
                    total=total_bytes,
                    desc=os.path.basename(filepath)
                ) as fout:
                    for chunk in response.iter_content(chunk_size=chunk_size):
                        if chunk:
                            fout.write(chunk)

                self.logger.info(f"Download concluído: {filepath}")
                return filepath

            except requests.RequestException as e:
                attempt += 1
                self.logger.warning(
                    f"Tentativa {attempt}/{max_retries} falhou: {e}"
                    )
                if attempt < max_retries:
                    sleep_time = backoff_factor ** attempt
                    self.logger.info(
                        f"Aguardando {sleep_time:.1f}s "
                        "antes da próxima tentativa..."
                    )
                    time.sleep(sleep_time)
                else:
                    self.logger.error(
                        f"Falha definitiva no download de {filename}: {e}"
                    )
                    return None

    def execute_download(
        self,
        satellite: str,
        lat: Optional[float],
        lon: Optional[float],
        tile_id: Optional[str],
        radius_km: Optional[float],
        start_date: str,
        end_date: str,
        tile_grid_path: str,
        max_cloud_cover: float
    ) -> None:
        """
        Executa todo o processo de busca, download e preparação da imagem.
        """
        bdc_conn = BdcConnection(self.logger).get_connection()
        fetcher = SatelliteImageFetcher(self.logger, bdc_conn)
        bbox_handler = BoundingBoxHandler(
            self.logger,
            reduction_factor=REDUCTION_FACTOR
        )

        uploader = MinioUploader(
            endpoint="localhost:9000",
            access_key="P8qQeeRKP6pHWDGuKiLi",
            secret_key="v7aKWRVPoN76hNQirzefTeeWsnSsNGHlz5AHI1QU",
            bucket_name="imagens-brutas",
            secure=False
        )

        year_month = datetime.strptime(
            start_date, "%Y-%m-%d"
        ).strftime("%Y-%m")
        self.output_dir = os.path.join(self.output_dir, satellite, year_month)
        self.create_output()

        if "landsat" in satellite.lower():
            tile_grid_path = SHAPEFILE_PATH_LANDSAT
            tiles_por_uf = LANDSAT_TILES_POR_UF
        elif "s2" in satellite.lower() or "sentinel" in satellite.lower():
            tiles_por_uf = SENTINEL_TILES_POR_UF

        if tile_id and tile_id.upper() in tiles_por_uf:
            uf = tile_id.upper()
            self.logger.info(f"Iniciando tiles do estado: {uf}")

            tile_list = tiles_por_uf.get(uf)
            if not tile_list:
                self.logger.warning(
                    f"Nenhum tile encontrado para {uf} com {satellite}"
                )
                raise ValueError(
                    f"Nenhum tile encontrado para {uf} com {satellite}"
                )

            TileProcessor(
                self.logger,
                fetcher,
                self,
                self.output_dir,
                tile_grid_path,
                max_cloud_cover,
                uploader
            ).process_tile_list(
                tile_list, satellite, start_date, end_date
            )
            return

        self.logger.info(tile_id)

        main_bbox, lat_final, lon_final, radius_final = (
            bbox_handler.obter_bounding_box(
                tile_id,
                lat,
                lon,
                radius_km,
                tile_grid_path,
                satellite
            )
        )

        image_assets = fetcher.fetch_image(
            satellite,
            main_bbox,
            start_date,
            end_date,
            max_cloud_cover,
            tile_grid_path,
            tile_id or ""
        )

        if not image_assets:
            print("Nenhuma imagem encontrada.")
            return

        prefix = (
            f"{tile_id}_{radius_final:.2f}"
            f"KM_{satellite}_{start_date}_{end_date}"
            if tile_id else
            f"{radius_final:.2f}KM_{satellite}_{lat_final:.3f}_"
            f"{lon_final:.3f}_{start_date}_{end_date}"
        )

        downloaded_files = DownloadBands(self.logger).download_bands(
            image_assets,
            self,
            prefix,
            satellite,
            uploader,
            tile_id or 'ponto'
        )

        # Prefixo no bucket pode conter data ou nome da tile
        for path in downloaded_files.values():
            uploader.upload_file(
                path,
                object_name=os.path.join(
                    satellite,
                    tile_id or 'ponto',
                    os.path.basename(path)
                )
            )
