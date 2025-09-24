# brazil_data_cube/downloader/image_downloader.py

import json
import logging
import os
from datetime import datetime
from typing import Optional

import requests
from tqdm import tqdm

from brazil_data_cube.config import (MINIO_ACCESS_KEY, MINIO_BUCKET,
                                     MINIO_ENDPOINT, MINIO_SECRET_KEY,
                                     MINIO_SECURE, REDUCTION_FACTOR,
                                     SHAPEFILE_PATH_BDC_MD,
                                     SHAPEFILE_PATH_LANDSAT,
                                     TILES_PATH_BDC_MD_V2, TILES_PATH_LANDSAT,
                                     TILES_PATH_SENTINEL)
from brazil_data_cube.downloader.download_bands import DownloadBands
from brazil_data_cube.downloader.fetcher import SatelliteImageFetcher
from brazil_data_cube.minio.MinioUploader import MinioUploader
from brazil_data_cube.processors.tile_processor import TileProcessor
from brazil_data_cube.utils.bdc_connection import BdcConnection
from brazil_data_cube.utils.bounding_box_handler import BoundingBoxHandler
from brazil_data_cube.utils.logger import ResultManager

with open(TILES_PATH_LANDSAT, "r", encoding="utf-8") as f:
    LANDSAT_TILES_POR_UF = json.load(f)

with open(TILES_PATH_SENTINEL, "r", encoding="utf-8") as f:
    SENTINEL_TILES_POR_UF = json.load(f)

with open(TILES_PATH_BDC_MD_V2, "r", encoding="utf-8") as f:
    BDC_MD_V2_TILES_POR_UF = json.load(f)


class ImageDownloader:
    def __init__(self, logger: logging.Logger, output_dir: str):
        self.output_dir = output_dir
        self.logger = logger
        self.create_output()
        self.remover_log = ResultManager.setup_deletion_logger()

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
        max_cloud_cover: float,
        min_geometry_cover: float
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
            endpoint=MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            bucket_name=MINIO_BUCKET,
            secure=MINIO_SECURE
        )

        year_month = datetime.strptime(
            start_date, "%Y-%m-%d"
        ).strftime("%Y-%m")
        self.output_dir = os.path.join(self.output_dir, satellite, year_month)
        self.create_output()
        caminho_minio = None
        mission = None
        sat = None
        level = None

        if "landsat" in satellite.lower():
            tile_grid_path = SHAPEFILE_PATH_LANDSAT
            tiles_por_uf = LANDSAT_TILES_POR_UF
            caminho_minio = "landsat"
            mission = "LANDSAT"
            sat = "L9"
            level = "LEVEL2"
        elif "s2" in satellite.lower() or "sentinel" in satellite.lower():
            tiles_por_uf = SENTINEL_TILES_POR_UF
            caminho_minio = "s2"
            mission = "SENTINEL2"
            sat = "S2A"
            level = "L2A"
        elif "cb" in satellite.lower():
            tile_grid_path = SHAPEFILE_PATH_BDC_MD
            tiles_por_uf = BDC_MD_V2_TILES_POR_UF
            caminho_minio = "cbers"
            mission = "CBERS"
            sat = "CB4"
            level = "SR"

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
                self.remover_log,
                fetcher,
                self,
                self.output_dir,
                tile_grid_path,
                max_cloud_cover,
                uploader,
                min_geometry_cover
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
            min_geometry_cover,
            tile_id or ""
        )

        if not image_assets:
            print("Nenhuma imagem encontrada.")
            return

        if tile_id is None:
            tile_id = image_assets.properties.get('bdc:tiles', [''])[0]

        data_criacao = image_assets.properties.get('created', '')

        image_assets = image_assets.assets

        if data_criacao:
            # Converte de ISO para datetime
            dt = datetime.fromisoformat(data_criacao.replace("Z", "+00:00"))
            # Formata para o padrão YYYYMMDD
            data_formatada = dt.strftime("%Y%m%d")
        else:
            data_formatada = "00000000"  # fallback

        prefix = (
                f"{sat}_{mission}_{tile_id}"
                f"_{data_formatada}_{level}"
            )

        downloaded_files = DownloadBands(self.logger).download_bands(
            image_assets,
            self,
            prefix,
            satellite,
            uploader,
            caminho_minio,
        )

        # Prefixo no bucket pode conter data ou nome da tile
        for path in downloaded_files.values():

            object_name = os.path.join(
                    caminho_minio,
                    os.path.basename(path)
                )

            uploader.upload_file(
                path,
                object_name=object_name
            )

            if uploader.object_exists(object_name, x=1):
                self.remover_log.info(
                    f"Arquivo no diretório {path} será deletado localmente"
                    )
                try:
                    os.remove(path)
                    self.remover_log.info(
                        f"Arquivo {path} deletado com sucesso."
                        )
                except FileNotFoundError:
                    self.remover_log.warning(
                        f"Arquivo {path} não encontrado para deletar."
                        )
                except Exception as e:
                    self.remover_log.error(
                        f"Erro ao deletar o arquivo {path}: {e}"
                        )
