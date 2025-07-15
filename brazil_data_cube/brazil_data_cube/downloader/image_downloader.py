# brazil_data_cube/downloader/image_downloader.py

import os
import requests
from tqdm import tqdm
import time
import logging
from datetime import datetime
from typing import Optional
from brazil_data_cube.utils.bdc_connection import BdcConnection
from brazil_data_cube.downloader.fetcher import SatelliteImageFetcher
from brazil_data_cube.utils.bounding_box_handler import BoundingBoxHandler
from brazil_data_cube.processors.tile_processor import TileProcessor
from brazil_data_cube.downloader.download_bandas import DownloadBandas
from brazil_data_cube.minio.MinioUploader import MinioUploader
from brazil_data_cube.config import REDUCTION_FACTOR, SHAPEFILE_PATH_LANDSAT


class ImagemDownloader:
    def __init__(self, logger: logging.Logger, output_dir: str):
        self.output_dir = output_dir
        self.logger = logger
        self.create_output()

    def create_output(self) -> None:
        """Cria diretório de saída se ele não existir."""
        os.makedirs(self.output_dir, exist_ok=True)
        self.logger.info(f"Diretório de saída criado em: {self.output_dir}")

    def download(self, asset: dict, filename: str, request_options: dict = {}) -> Optional[str]:
        """
        Baixa um asset usando requisição HTTP.
        
        Args:
            asset (dict): Asset do catálogo STAC
            filename (str): Nome do arquivo a ser salvo
            request_options (dict): Opções adicionais para o request

        Returns:
            Optional[str]: Caminho do arquivo baixado ou None se falhar
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
                response = requests.get(asset.href, stream=True, timeout=30, **request_options)
                response.raise_for_status()

                total_bytes = int(response.headers.get('content-length', 0))
                chunk_size = 1024 * 16

                with tqdm.wrapattr(open(filepath, 'wb'), 'write', miniters=1, total=total_bytes, desc=os.path.basename(filepath)) as fout:
                    for chunk in response.iter_content(chunk_size=chunk_size):
                        if chunk:
                            fout.write(chunk)

                self.logger.info(f"Download concluído: {filepath}")
                return filepath

            except (requests.RequestException, OSError) as e:
                attempt += 1
                self.logger.warning(f"Tentativa {attempt}/{max_retries} falhou: {e}")
                if attempt < max_retries:
                    sleep_time = backoff_factor ** attempt
                    self.logger.info(f"Aguardando {sleep_time:.1f}s antes da próxima tentativa...")
                    time.sleep(sleep_time)
                else:
                    self.logger.error(f"Falha definitiva no download de {filename}: {e}")
                    return None
        

    def executar_download(
            self,
            satelite: str,
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

            Args:
                satelite (str): Nome da coleção do satélite (ex: "S2_L2A-1").
                lat (float | None): Latitude do ponto central (se aplicável).
                lon (float | None): Longitude do ponto central (se aplicável).
                tile_id (str | None): ID do tile Sentinel ou nome do estado ("Paraná").
                radius_km (float | None): Raio de busca ao redor do ponto, em km.
                start_date (str): Data inicial no formato YYYY-MM-DD.
                end_date (str): Data final no formato YYYY-MM-DD.
                tile_grid_path (str): Caminho para o shapefile dos tiles.
                max_cloud_cover (float): Porcentagem máxima de cobertura de nuvens permitida.
            """
            # Conexão com o BDC (Brazil Data Cube)
            bdc_conn = BdcConnection(self.logger).get_connection()

            # Objeto responsável por buscar imagens via STAC
            fetcher = SatelliteImageFetcher(self.logger, bdc_conn)

            # Utilitário para gerar bounding box
            bbox_handler = BoundingBoxHandler(self.logger,reduction_factor=REDUCTION_FACTOR)

            ano_mes = datetime.strptime(start_date, "%Y-%m-%d").strftime("%Y-%m")
            self.output_dir = os.path.join(self.output_dir, satelite, ano_mes)
            self.create_output()

            if "landsat" in satelite.lower():
                tile_grid_path = SHAPEFILE_PATH_LANDSAT
            elif "s2" in satelite.lower() or "sentinel" in satelite.lower(): # Ex: "S2_L2A-1" ou "sentinel-2"
                tile_grid_path = tile_grid_path

            # Se for o estado do Paraná, delega ao TileProcessor
            if tile_id in ["Paraná", "parana"]:
                self.logger.info("Iniciando tiles do Paraná")
                TileProcessor(
                    self.logger,
                    fetcher,
                    self,  # passa o downloader atual
                    self.output_dir,
                    tile_grid_path,
                    max_cloud_cover
                ).processar_tiles_parana(satelite, start_date, end_date)
                return

            self.logger.info(tile_id)

            # Gera a bounding box com base nas coordenadas ou tile_id
            main_bbox, lat_final, lon_final, radius_final = bbox_handler.obter_bounding_box(
                tile_id, lat, lon, radius_km, tile_grid_path, satelite
            )

            # Busca as imagens dentro dos critérios definidos
            image_assets = fetcher.fetch_image(
                satelite,
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

            # Prefixo base para nomear arquivos
            prefixo = (
                f"{tile_id}_{radius_final:.2f}KM_{satelite}_{start_date}_{end_date}"
                if tile_id else
                f"{radius_final:.2f}KM_{satelite}_{lat_final:.3f}_{lon_final:.3f}_{start_date}_{end_date}"
            )

            # Faz o download das bandas RGB
            arquivos_baixados = DownloadBandas(self.logger).baixar_bandas(image_assets, self, prefixo,satelite)
            
            # Define o nome do arquivo final
            output_name = (
                f"{radius_final:.2f}KM_{satelite}_{tile_id}_{start_date}_{end_date}_RGB.tif"
                if tile_id else
                f"{radius_final:.2f}KM_{satelite}_{lat_final:.3f}_{lon_final:.3f}_{start_date}_{end_date}_RGB.tif"
            )

            # Caminho completo do arquivo final
            output_path = os.path.join(self.output_dir, output_name)

            # Aqui poderia vir o merge das bandas RGB se necessário
            # ImageProcessor(satelite).merge_rgb_tif(..., output_path)

            if arquivos_baixados:
                uploader = MinioUploader(
                    endpoint="localhost:9000",
                    access_key="P8qQeeRKP6pHWDGuKiLi",
                    secret_key="v7aKWRVPoN76hNQirzefTeeWsnSsNGHlz5AHI1QU",
                    bucket_name="imagens-brutas",
                    secure=False
                )
            
            data_range_folder = f"{start_date}_{end_date}"

            # Prefixo no bucket pode conter data ou nome da tile
            for path in arquivos_baixados.values():
                uploader.upload_file(path, object_name=os.path.join(satelite, tile_id or 'ponto', os.path.basename(path)))

