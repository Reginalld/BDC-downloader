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
                                     TILES_PATH_BDC_MD_V2, TILES_PATH_LANDSAT,
                                     TILES_PATH_SENTINEL)
from brazil_data_cube.downloader.download_bands import DownloadBands
from brazil_data_cube.downloader.fetcher import SatelliteImageFetcher
from brazil_data_cube.downloader.mission_info import MissionInfo
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

        for attempt in range(1, max_retries + 1):
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

        self.prepare_output_dir(satellite, start_date)

        mission_info = MissionInfo(satellite)

        if "sentinel-1-grd-bundle-1" in satellite:
            self.logger.info("Processando Sentinel-1 (modo radar SAR).")

            # Bounding box é obrigatória (lat/lon ou tile_id → convertido para bbox)
            bbox, lat, lon, _ = bbox_handler.obter_bounding_box(
                tile_id,
                lat,
                lon,
                radius_km,
                mission_info.tile_grid_path,
                mission_info.sat
            )

            image_assets = fetcher.fetch_image(
                mission_info.sat,
                bbox,
                start_date,
                end_date,
                max_cloud_cover=0,        # ignorado internamente
                tile_grid_path="",        # sem uso
                min_geometry_cover=0.0,   # sem uso
                tile=None,
            )

            if not image_assets:
                self.logger.warning("Nenhuma imagem Sentinel-1 encontrada.")
                return

            # Usa timestamp como identificador
            data_criacao = image_assets.properties.get("start_datetime", "")
            data_formatada = (
                datetime.fromisoformat(data_criacao.replace("Z", "+00:00")).strftime("%Y%m%d")
                if data_criacao else "00000000"
            )

            prefix = f"{mission_info.sat}_{mission_info.mission}_{lat}_{lon}_{data_formatada}_{mission_info.level}"

            download_files = DownloadBands(self.logger).download_bands(
                image_assets.assets,
                self,
                prefix,
                mission_info.sat,
                uploader,
                mission_info.bucket_prefix,
            )

            for path in download_files.values():
                self.upload_and_cleanup(uploader, path, mission_info.bucket_prefix)

            return

        if tile_id and tile_id.upper() in mission_info.tiles_por_uf:
            self.process_tiles_por_estado(
                tile_id.upper(),
                mission_info,
                fetcher,
                uploader,
                start_date,
                end_date,
                max_cloud_cover,
                min_geometry_cover,
            )
            return

        self.process_single_tile(
            tile_id,
            lat,
            lon,
            radius_km,
            bbox_handler,
            fetcher,
            uploader,
            mission_info,
            mission_info.tile_grid_path,
            start_date,
            end_date,
            max_cloud_cover,
            min_geometry_cover,
        )

    def prepare_output_dir(self, satellite: str, start_date: str) -> None:
        """Cria diretório de saída baseado em satélite e data."""
        year_month = datetime.strptime(
            start_date, "%Y-%m-%d").strftime("%Y-%m")
        self.output_dir = os.path.join(
            self.output_dir, satellite, year_month
            )
        self.create_output()

    def process_tiles_por_estado(
        self,
        uf: str,
        mission_info: MissionInfo,
        fetcher: SatelliteImageFetcher,
        uploader: MinioUploader,
        start_date: str,
        end_date: str,
        max_cloud_cover: float,
        min_geometry_cover: float,
    ) -> None:
        """Processa lista de tiles de um estado (UF)"""
        tile_list = mission_info.tiles_por_uf.get(uf)
        if not tile_list:
            msg = f"Nenhum tile encontrado para {uf} ({mission_info.mission})"
            self.logger.error(msg)
            raise ValueError(msg)

        self.logger.info(f"Iniciando tiles do estado {uf}...")
        TileProcessor(
            self.logger,
            self.remover_log,
            fetcher,
            self,
            self.output_dir,
            mission_info.tile_grid_path,
            max_cloud_cover,
            uploader,
            min_geometry_cover,
        ).process_tile_list(tile_list, mission_info.sat, start_date, end_date)

    def process_single_tile(
        self,
        tile_id: Optional[str],
        lat: Optional[float],
        lon: Optional[float],
        radius_km: Optional[float],
        bbox_handler: BoundingBoxHandler,
        fetcher: SatelliteImageFetcher,
        uploader: MinioUploader,
        mission_info: MissionInfo,
        tile_grid_path: str,
        start_date: str,
        end_date: str,
        max_cloud_cover: float,
        min_geometry_cover: float,
    ) -> None:
        """Processa o fluxo para um único tile (ou bbox)."""
        bbox, lat, lon, _ = bbox_handler.obter_bounding_box(
            tile_id, lat, lon, radius_km,
            mission_info.tile_grid_path, mission_info.sat
        )

        image_assets = fetcher.fetch_image(
            mission_info.sat,
            bbox,
            start_date,
            end_date,
            max_cloud_cover,
            tile_grid_path,
            min_geometry_cover,
            tile_id or "",
        )
        if not image_assets:
            self.logger.warning("Nenhuma imagem encontrada.")
            return

        if not tile_id:
            tile_id = image_assets.properties.get("bdc:tiles", [""])[0]

        # Data de criação
        data_criacao = image_assets.properties.get("start_datetime", "")
        data_formatada = (
            datetime.fromisoformat(
                data_criacao.replace("Z", "+00:00")).strftime("%Y%m%d")
            if data_criacao
            else "00000000"
        )

        prefix = f"{mission_info.sat}_{mission_info.mission}_" \
            f"{tile_id}_{data_formatada}_{mission_info.level}"

        download_files = DownloadBands(self.logger).download_bands(
            image_assets.assets,
            self,
            prefix,
            mission_info.sat,
            uploader,
            mission_info.bucket_prefix,
        )

        for path in download_files.values():
            self.upload_and_cleanup(uploader, path, mission_info.bucket_prefix)

    def upload_and_cleanup(
            self, uploader: MinioUploader, filepath: str, bucket_prefix: str
            ) -> None:
        """Faz upload para MinIO e remove o
        arquivo local se já existir no bucket."""
        object_name = os.path.join(bucket_prefix, os.path.basename(filepath))

        uploader.upload_file(filepath, object_name=object_name)

        if uploader.object_exists(object_name, x=1):
            self.remover_log.info(f"Removendo local: {filepath}")
            try:
                os.remove(filepath)
                self.remover_log.info(f"Arquivo {filepath} "
                                      "deletado com sucesso.")
            except FileNotFoundError:
                self.remover_log.warning(f"Arquivo {filepath} não encontrado.")
            except Exception as e:
                self.remover_log.error(f"Erro ao deletar {filepath}: {e}")
