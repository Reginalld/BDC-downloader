import os
import requests
from tqdm import tqdm
import logging
from typing import Optional


logger = logging.getLogger(__name__)

class DownloadBandas:
    def __init__(self):
        pass
    
    @staticmethod
    def baixar_bandas(image_assets, downloader , prefixo,satelite):
        """
        Função responsável pela chamada de download de cada banda, evitando repetição onde necessário.

        """
        if satelite == "S2_L2A-1":
            bandas = {
                'B04': 'red',
                'B03': 'green',
                'B02': 'blue',
                'AOT': 'AOT',
                'B01': 'B01',
                'B05': 'B05',
                'B06': 'B06',
                'B07': 'B07',
                'B08': 'B08',
                'B09': 'B09',
                'B11': 'B11',
                'B12': 'B12',
                'B8A': 'B8A',
                'PVI': 'PVI',
                'SCL': 'SCL',
                'TCI': 'TCI',
                'WVP': 'WVP',
                'MTD_TL': 'MTD_TL'
            }
        elif satelite == "landsat-2":
            bandas = {
                'ang': 'ang',
                'red': 'red',
                'blue': 'blue',
                'green': 'green',
                'nir08': 'nir08',
                'st_qa': 'st_qa',
                'lwir11': 'lwir11',
                'swir16': 'swir16',
                'swir22': 'swir22',
                'coastal': 'coastal',
                'mtl.txt': 'mtl.txt',
                'mtl.xml': 'mtx.xml',
                'st_drad': 'st_drad',
                'st_emis': 'st_emis',
                'st_emsd': 'st_emsd',
                'st_trad': 'st_trad',
                'st_urad': 'st_urad',
                'qa_pixel': 'qa_pixel',
                'st_atran': 'st_atran',
                'st_cdist': 'st_cdist',
                'qa_radsat': 'qa_radsat',
                'thumbnail': 'thumbnail',
                'qa_aerosol': 'qa_aerosol'
            }

        arquivos_baixados = {}
        for banda, sufixo in bandas.items():
            if banda in image_assets:
                try:
                    filepath = downloader.download(image_assets[banda], f"{prefixo}_{sufixo}")
                    if filepath:
                        arquivos_baixados[banda] = filepath
                    else:
                        logger.warning(f"Download falhou para banda '{banda}' ({sufixo})")
                except Exception as e:
                    logger.error(f"Erro ao baixar banda '{banda}': {e}")
        return arquivos_baixados
