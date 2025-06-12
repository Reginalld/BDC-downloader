# brazil_data_cube/downloader/image_downloader.py

import os
import requests
from tqdm import tqdm
import logging
from typing import Optional


logger = logging.getLogger(__name__)

class ImagemDownloader:
    def __init__(self, output_dir: str):
        self.output_dir = output_dir
        self.create_output()

    def create_output(self) -> None:
        """Cria diretório de saída se ele não existir."""
        os.makedirs(self.output_dir, exist_ok=True)
        logger.info(f"Diretório de saída criado em: {self.output_dir}")

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
        try:
            if asset is None:
                logger.error("Tentativa de download com asset inválido.")
                raise ValueError("Asset inválido.")

            filepath = os.path.join(self.output_dir, filename)
            logger.info(f"Iniciando download da imagem para: {filepath}")

            response = requests.get(asset.href, stream=True, **request_options)
            total_bytes = int(response.headers.get('content-length', 0))
            chunk_size = 1024 * 16

            with tqdm.wrapattr(open(filepath, 'wb'), 'write', miniters=1, total=total_bytes, desc=os.path.basename(filepath)) as fout:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    fout.write(chunk)

            logger.info(f"Download concluído: {filepath}")
            return filepath

        except Exception as e:
            logger.error(f"Erro ao fazer download da imagem: {str(e)}")
            raise RuntimeError(f"Erro ao fazer download da imagem: {e}")