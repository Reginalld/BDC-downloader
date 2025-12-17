# brazil_data_cube/downloader/image_downloader.py

import json
import logging
import os
from datetime import datetime
from typing import Optional


from brazil_data_cube.config import (MINIO_ACCESS_KEY, MINIO_BUCKET,
                                     MINIO_ENDPOINT, MINIO_SECRET_KEY,
                                     MINIO_SECURE, REDUCTION_FACTOR,
                                     SHAPEFILE_PATH, SHAPEFILE_PATH_BDC_MD,
                                     SHAPEFILE_PATH_LANDSAT,
                                     TILES_PATH_BDC_MD_V2, TILES_PATH_LANDSAT,
                                     TILES_PATH_SENTINEL)
from brazil_data_cube.downloader.download_bands import DownloadBands
from brazil_data_cube.downloader.fetcher import SatelliteImageFetcher
from brazil_data_cube.minio.MinioUploader import MinioUploader
from brazil_data_cube.processors.tile_processor import TileProcessor
from brazil_data_cube.services.db_writer import DatabaseRecorder
from brazil_data_cube.utils.bdc_connection import BdcConnection
from brazil_data_cube.utils.bounding_box_handler import BoundingBoxHandler
from brazil_data_cube.utils.mission_info import MissionInfo
from brazil_data_cube.services.scene_persister import ScenePersister
from brazil_data_cube.downloader.http_downloader import HttpDownloader

with open(TILES_PATH_LANDSAT, "r", encoding="utf-8") as f:
    LANDSAT_TILES_POR_UF = json.load(f)

with open(TILES_PATH_SENTINEL, "r", encoding="utf-8") as f:
    SENTINEL_TILES_POR_UF = json.load(f)

with open(TILES_PATH_BDC_MD_V2, "r", encoding="utf-8") as f:
    BDC_MD_V2_TILES_POR_UF = json.load(f)


TILES_PATHS_CONFIG = {
            "CB4": SHAPEFILE_PATH_BDC_MD,
            "L8": SHAPEFILE_PATH_LANDSAT,
            "S1A": SHAPEFILE_PATH_BDC_MD,
            "S2A": SHAPEFILE_PATH,
        }


class ImageDownloader:
    def __init__(self, logger: logging.Logger, output_dir: str):
        self.logger = logger
        self.output_dir = output_dir

        self.http_downloader = HttpDownloader(logger)

        self.db_recorder = DatabaseRecorder(
            logger=logger,
            session_factory=None,
            tile_paths=TILES_PATHS_CONFIG
        )

        self.uploader = MinioUploader(
            endpoint=MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            bucket_name=MINIO_BUCKET,
            secure=MINIO_SECURE
        )

        self.scene_persister = ScenePersister(
            logger=logger,
            db_recorder=self.db_recorder,
            uploader=self.uploader
        )

        self.create_output()

    def create_output(self) -> None:
        """Cria diretório de saída base se ele não existir."""
        os.makedirs(self.output_dir, exist_ok=True)
        self.logger.info(f"Diretório base verificado: {self.output_dir}")

    def prepare_output_dir(self, satellite: str, start_date: str) -> None:
        """Atualiza o diretório de saída para incluir satélite/ano-mês."""
        year_month = datetime.strptime(
            start_date, "%Y-%m-%d").strftime("%Y-%m")
        self.output_dir = os.path.join(self.output_dir, satellite, year_month)
        self.create_output()

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
        Ponto de entrada principal. Configura conexões e roteia para
        processamento de tile único ou lista de estados.
        """
        bdc_conn = BdcConnection(self.logger).get_connection()
        fetcher = SatelliteImageFetcher(self.logger, bdc_conn)
        bbox_handler = BoundingBoxHandler(
            self.logger, reduction_factor=REDUCTION_FACTOR)
        mission_info = MissionInfo(satellite)

        # Prepara estrutura de pastas
        self.prepare_output_dir(satellite, start_date)

        # Roteamento: Estado vs Single Tile
        if tile_id and tile_id.upper() in mission_info.tiles_por_uf:
            self.process_tiles_por_estado(
                tile_id.upper(),
                mission_info,
                fetcher,
                self.uploader,
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
            self.uploader,
            mission_info,
            mission_info.tile_grid_path,
            start_date,
            end_date,
            max_cloud_cover,
            min_geometry_cover,
        )

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
        """Delega o loop de tiles do estado para o TileProcessor."""
        tile_list = mission_info.tiles_por_uf.get(uf)
        if not tile_list:
            msg = f"Nenhum tile encontrado para {uf} ({mission_info.mission})"
            self.logger.error(msg)
            raise ValueError(msg)

        self.logger.info(f"Iniciando tiles do estado {uf}...")
        TileProcessor(
            self.logger,
            fetcher,
            self,  # Passa a própria instância (Orquestrador)
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
        """
        Orquestra o fluxo: BBox -> Fetch Metadata -> Download -> Persist.
        """
        # 1. Obter Bounding Box
        bbox, lat, lon, _ = bbox_handler.obter_bounding_box(
            tile_id, lat, lon, radius_km,
            mission_info.tile_grid_path, mission_info.sat
        )

        # 2. Buscar Imagens (Metadata)
        image_assets = fetcher.fetch_image(
            mission_info.sat, bbox, start_date, end_date,
            max_cloud_cover, tile_grid_path, min_geometry_cover,
            tile_id or ""
        )
        if not image_assets:
            self.logger.warning("Nenhuma imagem encontrada.")
            return

        # 3. Ajustes de Tile ID e BBox (S1A footprints)
        if "S1A" in mission_info.sat and tile_id is None:
            extracted_bbox = bbox_handler.extract_bbox_from_footprint(
                image_assets)
            if extracted_bbox:
                bbox = extracted_bbox
                tile_id = bbox_handler.make_tile_id_from_bbox(bbox)

        if not tile_id:
            tile_id = image_assets.properties.get("bdc:tiles", [""])[0]

        # 4. Formatação de Data/Prefixo
        data_criacao = image_assets.properties.get("start_datetime", "")
        if data_criacao:
            dt_obj = datetime.fromisoformat(
                data_criacao.replace("Z", "+00:00"))
            data_formatada = dt_obj.strftime("%Y%m%d")
        else:
            data_formatada = "00000000"

        prefix = (
            f"{mission_info.sat}_"
            f"{mission_info.mission}_"
            f"{tile_id}_"
            f"{data_formatada}_"
            f"{mission_info.level}"
        )

        download_files = DownloadBands(self.logger).download_bands(
            image_assets.assets,
            self.http_downloader,
            prefix,
            mission_info.sat,
            uploader,
            mission_info.bucket_prefix,
            output_dir=self.output_dir
        )

        if not download_files:
            self.logger.warning("Nenhum arquivo para upload.")
            return

        self.scene_persister.persist_batch(
            download_files=download_files,
            mission_info=mission_info,
            tile_id=tile_id,
            start_datetime_str=data_criacao,
            bbox=bbox
        )
