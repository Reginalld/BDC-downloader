import logging
import os


class DownloadBands:
    def __init__(self, logger: logging.Logger):
        self.logger = logger

    def download_bands(
            self, image_assets, downloader,
            prefix, satellite, minio_uploader,
            caminho_minio
            ):
        """
        Função responsável pela chamada de download de cada banda,
        evitando repetição onde necessário.
        """
        if "S2" in satellite.upper():
            bands = {
                'B04': 'RED',
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
                # 'SCL': 'SCL',
                # 'TCI': 'TCI',
                # 'WVP': 'WVP',
                # 'MTD_TL': 'MTD_TL'
            }
        elif "L8" in satellite.upper():
            bands = {
                'ang': 'ANG',
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
                # 'thumbnail': 'THUMBNAIL',
                # 'qa_aerosol': 'QA_AEROSOL'
            }
        elif "CB" in satellite.upper():
            bands = {
                'EVI': 'EVI',
                # 'NDVI': 'NDVI',
                # 'BAND5': 'BLUE',
                # 'BAND6': 'GREEN',
                # 'BAND7': 'RED',
                # 'BAND8': 'NIR08',
                # 'CMASK': 'CMASK',
                # 'CLEAROB': 'CLEAROB',
                # 'TOTALOB': 'TOTALOB',
                # 'thumbnail': 'THUMBNAIL',
                # 'PROVENANCE': 'PROVENANCE'
            }

        download_files = {}

        for band, suffix in bands.items():
            if band in image_assets:
                filename = f"{prefix}_{suffix}.tif"
                object_name = os.path.join(
                   caminho_minio, filename
                    ).replace("\\", "/")
                print(object_name)
                # Verifica se já existe no MinIO
                if minio_uploader.object_exists(object_name, x=0):
                    continue

                try:
                    filepath = downloader.download(
                        image_assets[band], filename
                        )

                    if filepath:
                        download_files[band] = filepath
                    else:
                        self.logger.warning(
                            f"Download falhou para banda '{band}' ({suffix})"
                            )
                except Exception as e:
                    self.logger.error(f"Erro ao baixar banda '{band}': {e}")

        return download_files
