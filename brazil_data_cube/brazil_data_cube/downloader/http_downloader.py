# brazil_data_cube/downloader/http_downloader.py

import os
import logging
import requests
from tqdm import tqdm
from typing import Optional


class HttpDownloader:
    def __init__(self, logger: logging.Logger):
        self.logger = logger

    def download_file(
        self,
        url: str,
        output_path: str,
        timeout: int = 30,
        max_retries: int = 3,
        request_options: dict = None
    ) -> Optional[str]:
        """
        Baixa um arquivo de uma URL para um caminho local com retries e backoff
        """
        if not url:
            self.logger.error("URL vazia fornecida para download.")
            return None

        request_options = request_options or {}

        # Garante que o diretório pai existe
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        backoff_factor = 2.0

        self.logger.info(f"Iniciando download: {url} -> {output_path}")

        for attempt in range(1, max_retries + 1):
            try:
                response = requests.get(
                    url, stream=True, timeout=timeout, **request_options)
                response.raise_for_status()

                total_bytes = int(response.headers.get('content-length', 0))
                chunk_size = 1024 * 16

                with tqdm.wrapattr(
                    open(output_path, 'wb'),
                    'write',
                    miniters=1,
                    total=total_bytes,
                    desc=os.path.basename(output_path)
                ) as fout:
                    for chunk in response.iter_content(chunk_size=chunk_size):
                        if chunk:
                            fout.write(chunk)

                return output_path

            except requests.RequestException as e:
                self.logger.warning(
                    f"Tentativa {attempt}/{max_retries} falhou: {e}")
                if attempt < max_retries:
                    import time
                    sleep_time = backoff_factor ** attempt
                    time.sleep(sleep_time)
                else:
                    self.logger.error(
                        f"Falha definitiva no download de {output_path}: {e}")
                    # Remover arquivo parcial se falhar
                    if os.path.exists(output_path):
                        os.remove(output_path)
                    return None
