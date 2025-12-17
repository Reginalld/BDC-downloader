import logging
import os

import requests
from tqdm import tqdm


class DownloadBands:
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
        Download com suporte a resume, lógica de status code corrigida e
        verificação de tamanho total primeiro.
        """
        os.makedirs(output_dir, exist_ok=True)
        output_file = os.path.join(output_dir, filename)
        url = asset.href if hasattr(asset, "href") else asset

        total_size = 0

        try:
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

        while True:
            existing_size = os.path.getsize(output_file) \
                if os.path.exists(output_file) else 0

            if total_size > 0 and existing_size >= total_size:
                self.logger.info(
                    f"Arquivo {filename} já está completo "
                    f"({existing_size}/{total_size} bytes). Pulando."
                    )

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
        Função responsável pela chamada de download de cada banda,
        evitando repetição onde necessário.
        """
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
            if band in image_assets:
                filename = f"{prefix}_{suffix}.tif"
                if satellite == "S1A":
                    filename = f"{prefix}_{suffix}.zip"
                object_name = os.path.join(
                   caminho_minio, filename
                    ).replace("\\", "/")
                print(object_name)
                # Verifica se já existe no MinIO
                if minio_uploader.object_exists(object_name, x=0):
                    continue

                filepath_local = os.path.join(output_dir, filename)

                try:
                    if satellite.upper() == "S1A":
                        filepath = self.download_with_resume(
                            image_assets[band], filename,
                            output_dir=output_dir)
                    else:
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
