# brazil_data_cube/downloader/http_downloader.py

import os
import logging
import requests
from tqdm import tqdm
from typing import Optional


class HttpDownloader:
    """
    Gerenciador de transferências HTTP resiliente.

    Esta classe encapsula a biblioteca `requests`, adicionando camadas de proteção
    contra instabilidades de rede, como retentativas automáticas (retries),
    espera exponencial (backoff) e visualização de progresso.

    Attributes:
        logger (logging.Logger): Logger para registro de eventos e falhas.
    """
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
        Realiza o download de um arquivo remoto com estratégia de resiliência.

        O método implementa um loop de retentativa com 'Exponential Backoff'.
        Se a conexão falhar, ele espera 2s, depois 4s, depois 8s, etc.,
        evitando sobrecarregar o servidor remoto.

        Também utiliza `stream=True` para baixar o arquivo em pedaços (chunks),
        garantindo que arquivos maiores que a RAM (ex: 2GB) não causem MemoryOverflow.

        Args:
            url (str): URL de origem do arquivo.
            output_path (str): Caminho local completo onde o arquivo será salvo.
            timeout (int, optional): Tempo máximo de espera (em segundos) para conectar. Default: 30.
            max_retries (int, optional): Número máximo de tentativas em caso de erro. Default: 3.
            request_options (dict, optional): Dicionário de argumentos extras para `requests.get` (ex: headers/auth).

        Returns:
            Optional[str]: O caminho absoluto do arquivo salvo em caso de sucesso, ou None se falhar definitivamente.
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
                # stream=True.
                # Ele baixa os headers mas mantém a conexão aberta, baixando o corpo sob demanda.
                response = requests.get(
                    url, stream=True, timeout=timeout, **request_options)
                response.raise_for_status()

                total_bytes = int(response.headers.get('content-length', 0))
                chunk_size = 1024 * 16

                # Abre o arquivo em modo binário de escrita ('wb')
                # Envolve com tqdm para mostrar barra de progresso no terminal/log
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

                # Sucesso: retorna o caminho do arquivo
                return output_path

            except requests.RequestException as e:
                self.logger.warning(
                    f"Tentativa {attempt}/{max_retries} falhou: {e}")

                # Se ainda houver tentativas, aplica o Backoff
                if attempt < max_retries:
                    import time
                    sleep_time = backoff_factor ** attempt
                    time.sleep(sleep_time)
                else:
                    self.logger.error(
                        f"Falha definitiva no download de {output_path}: {e}")

                    # Limpeza: Se o arquivo ficou pela metade (corrompido), deleta.
                    if os.path.exists(output_path):
                        os.remove(output_path)
                    return None
