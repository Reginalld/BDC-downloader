import os
import requests
from tqdm import tqdm
import logging
from typing import Optional


logger = logging.getLogger(__name__)

class DownloadBandas:
    def __init__(self):
        pass

    def baixar_bandas(image_assets, downloader , prefixo):
        """
        Função responsável pela chamada de download de cada banda, evitando repetição onde necessário.

        """

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

        arquivos_baixados = {}
        for banda, sufixo in bandas.items():
            if banda in image_assets:
                arquivos_baixados[banda] = downloader.download(image_assets[banda], f"{prefixo}_{sufixo}")
        return arquivos_baixados
