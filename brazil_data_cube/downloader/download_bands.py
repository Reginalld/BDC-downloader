import logging
import os

import requests
from tqdm import tqdm


class DownloadBands:
    """
    Gerenciador de materialização de ativos (assets).

    Responsável por baixar arquivos do Stac para local.
    Sua principal característica é o 'Resume': capacidade de
    continuar downloads interrompidos sem corromper o arquivo, essencial para
    arquivos ZIP de Radar (>1GB).

    Attributes:
        logger (logging.Logger): Instância de logger.
    """
    def __init__(self, logger: logging.Logger):
        self.logger = logger

    def download_with_resume(
            self,
            asset,
            filename: str,
            output_dir: str,
            chunk_size: int = 1024*1024,
            timeout: int = 60
            ) -> str:
        """
        Executa download resiliente com suporte a retomada (HTTP Range).

        Implementa um loop de controle que verifica o tamanho do arquivo local
        versus remoto. Se o arquivo local for menor, solicita apenas os bytes
        faltantes (Header `Range: bytes=N-`).

        Logica de Controle:
        1. Handshake: Obtém tamanho total (`Content-Length`) via GET inicial.
        2. Verificação: Compara com arquivo em disco.
        3. Retomada: Se incompleto, envia header `Range`.
        4. Validação: Aceita HTTP 206 (Partial) ou 200 (Reinício).

        Args:
            asset (Any): Objeto asset do STAC ou URL direta.
            filename (str): Nome do arquivo de destino.
            output_dir (str): Diretório local de salvamento.
            chunk_size (int, optional): Tamanho do buffer de escrita.
            timeout (int, optional): Timeout de socket em segundos.

        Returns:
            str: Caminho completo do arquivo baixado.

        Raises:
            requests.exceptions.RequestException: Em caso de falha fatal de rede. # noqa: E501
        """
        os.makedirs(output_dir, exist_ok=True)
        output_file = os.path.join(output_dir, filename)

        # Extrai URL (compatibilidade com pystac Item ou string pura)
        url = asset.href if hasattr(asset, "href") else asset

        total_size = 0

        # Etapa 1: Obter o tamanho total do arquivo remoto
        try:
            # stream=True baixa apenas headers inicialmente
            initial_response = requests.get(url, stream=True, timeout=timeout)
            initial_response.raise_for_status()  # Garante que o link é válido

            total_size = int(initial_response.headers.get('content-length', 0))
            initial_response.close()  # Fecha a conexão

            if total_size == 0:
                self.logger.warning(
                    f"Servidor não retornou 'content-length' "
                    f"para {filename}. O resume pode falhar.")

        except requests.exceptions.RequestException as e:
            self.logger.error(
                f"Erro fatal ao tentar obter tamanho do arquivo {url}: {e}"
                )
            raise

        # Loop de Download/Retomada
        while True:
            # Verifica progresso atual
            existing_size = os.path.getsize(output_file) \
                if os.path.exists(output_file) else 0

            # Condição de Saída 1: Arquivo já está completo
            if total_size > 0 and existing_size >= total_size:
                self.logger.info(
                    f"Arquivo {filename} já está completo "
                    f"({existing_size}/{total_size} bytes). Pulando."
                    )

                # Sanity Check.
                if existing_size > total_size:
                    self.logger.error(
                        f"CORRUPÇÃO DETECTADA: Arquivo local {filename} "
                        f"({existing_size}) é MAIOR que o esperado "
                        f"({total_size}). Deletando e baixando novamente."
                        )
                    os.remove(output_file)
                    existing_size = 0
                else:
                    break

            # Prepara headers para Resume
            headers = {}
            if existing_size > 0:
                headers["Range"] = f"bytes={existing_size}-"
                self.logger.info(
                    f"Tentando retomar download de {filename} "
                    f"a partir de {existing_size} bytes."
                    )

            try:
                response = requests.get(
                    url, stream=True, timeout=timeout, headers=headers)

                # Lógica de validação do status HTTP
                if existing_size > 0:
                    if response.status_code == 206:
                        mode = 'ab'
                        self.logger.info(
                            "Servidor aceitou 'resume' (206). Continuando..."
                            )
                    elif response.status_code == 200:
                        self.logger.warning(
                            "Servidor IGNOROU 'resume' (retornou 200). "
                            "Reiniciando download do zero."
                            )
                        existing_size = 0
                        mode = 'wb'
                    else:
                        response.raise_for_status()
                else:
                    if response.status_code != 200:
                        self.logger.error(
                            f"Erro ao iniciar novo download. Esperado "
                            f"200 OK, mas recebi {response.status_code}"
                            )
                        response.raise_for_status()
                    mode = 'wb'

                # Escrita do arquivo com barra de progresso
                with open(output_file, mode) as fout:
                    with tqdm(
                        total=total_size, initial=existing_size,
                        desc=filename, unit='B', unit_scale=True, miniters=1,
                        unit_divisor=1024
                    ) as pbar:
                        for chunk in response.iter_content(
                                        chunk_size=chunk_size):
                            if chunk:
                                fout.write(chunk)
                                pbar.update(len(chunk))

                # Condição de Saída 2: Download finalizou neste ciclo
                current_size = os.path.getsize(output_file)
                if total_size > 0 and current_size < total_size:
                    self.logger.warning(
                        f"Download ainda incompleto ({current_size}/"
                        f"{total_size}). O loop vai tentar novamente..."
                        )
                else:
                    self.logger.info(
                        f"Download de {filename} parece "
                        f"concluído ({current_size}/{total_size})."
                        )
                    break

            except requests.exceptions.ChunkedEncodingError as e:
                self.logger.warning(
                    f"Erro de conexão (ChunkedEncodingError) durante "
                    f"download de {filename}: {e}. Tentando novamente..."
                    )
            except requests.exceptions.RequestException as e:
                self.logger.error(
                    f"Erro de request fatal durante "
                    f"download de {filename}: {e}. Abortando."
                    )
                raise

        return output_file

    def download_bands(
            self, image_assets, downloader,
            prefix, satellite, minio_uploader,
            caminho_minio, output_dir
            ):
        """
        Orquestra o download em lote das bandas de uma cena.

        Aplica filtragem (Whitelist) para baixar apenas arquivos relevantes
        e verifica existência no MinIO (Idempotência) antes de baixar.

        Args:
            image_assets (Dict[str, Any]): Dicionário de assets do item STAC.
            downloader (HttpDownloader): Cliente HTTP padrão.
            prefix (str): Nome base padronizado para os arquivos.
            satellite (str): Identificador do satélite.
            minio_uploader (MinioUploader): Cliente MinIO para verificação de existência de imagem. # noqa: E501
            caminho_minio (str): Caminho "virtual" dentro do bucket de destino.
            output_dir (str): Diretório local de saída.

        Returns:
            Dict[str, str]: Mapa de {nome_banda: caminho_local} dos arquivos baixados.
        """
        # Configuração de Whitelist (Bandas de Interesse)
        if "S2" in satellite.upper():
            bands = {
                # 'B04': 'RED',
                # 'B03': 'GREEN',
                # 'B02': 'BLUE',
                # 'AOT': 'AOT',
                # 'B01': 'B01',
                # 'B05': 'B05',
                # 'B06': 'B06',
                # 'B07': 'B07',
                # 'B08': 'B08',
                # 'B09': 'B09',
                # 'B11': 'B11',
                # 'B12': 'B12',
                # 'B8A': 'B8A',
                # 'PVI': 'PVI',
                'SCL': 'SCL',
                # 'TCI': 'TCI',
                # 'WVP': 'WVP',
                # 'MTD_TL': 'MTD_TL'
            }
        elif "L8" in satellite.upper():
            bands = {
                # 'ang': 'ANG',
                # 'red': 'RED',
                # 'blue': 'BLUE',
                # 'green': 'GREEN',
                # 'nir08': 'NIR08',
                # 'st_qa': 'ST_QA',
                # 'lwir11': 'LWIR11',
                # 'swir16': 'SWIR16',
                # 'swir22': 'SWIR22',
                # 'coastal': 'COASTAL',
                # 'mtl.txt': 'MTL.txt',
                # 'mtl.xml': 'MTX.xml',
                # 'st_drad': 'ST_DRAD',
                # 'st_emis': 'ST_EMIS',
                # 'st_emsd': 'ST_EMSD',
                # 'st_trad': 'ST_TRAD',
                # 'st_urad': 'ST_URAD',
                # 'qa_pixel': 'QA_PIXEL',
                # 'st_atran': 'ST_ATRAN',
                # 'st_cdist': 'ST_CDIST',
                # 'qa_radsat': 'QA_RADSAT',
                'thumbnail': 'THUMBNAIL',
                # 'qa_aerosol': 'QA_AEROSOL'
            }
        elif "CB" in satellite.upper():
            bands = {
                # 'BAND6': 'GREEN',
                # 'EVI': 'EVI',
                # 'NDVI': 'NDVI',
                # 'BAND5': 'BLUE',
                # 'BAND7': 'RED',
                # 'BAND8': 'NIR08',
                # 'CMASK': 'CMASK',
                # 'CLEAROB': 'CLEAROB',
                # 'TOTALOB': 'TOTALOB',
                'thumbnail': 'THUMBNAIL',
                # 'PROVENANCE': 'PROVENANCE'
            }
        elif "S1A" in satellite.upper():
            bands = {
                'asset': 'asset',
            }

        download_files = {}

        for band, suffix in bands.items():
            # Verifica se a banda desejada existe no item STAC atual
            if band in image_assets:
                # Define extensão baseada no tipo de dado
                filename = f"{prefix}_{suffix}.tif"
                if satellite == "S1A":
                    filename = f"{prefix}_{suffix}.zip"
                object_name = os.path.join(
                   caminho_minio, filename
                    ).replace("\\", "/")
                # Se o arquivo já está salvo e no MinIO, não baixa novamente.
                if minio_uploader.object_exists(object_name, x=0):
                    continue

                filepath_local = os.path.join(output_dir, filename)

                try:
                    # Roteamento de Estratégia de Download
                    if satellite.upper() == "S1A":
                        # Radar: Arquivos gigantes (ZIP) -> Usa Resume manual
                        filepath = self.download_with_resume(
                            image_assets[band], filename,
                            output_dir=output_dir)
                    else:
                        # Óptico: Arquivos menores -> Usa HttpDownloader padrão
                        # Extrai URL do objeto Asset
                        asset = image_assets[band]
                        url = asset.href if hasattr(asset, "href") else asset

                        filepath = downloader.download_file(
                            url=url,
                            output_path=filepath_local
                        )

                    if filepath:
                        download_files[band] = filepath
                    else:
                        self.logger.warning(f"Download falhou para "
                                            f"banda '{band}' ({suffix})")
                except Exception as e:
                    self.logger.error(f"Erro ao baixar banda '{band}': {e}")

        return download_files
