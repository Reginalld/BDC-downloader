# brazil_data_cube/processors/image_processor.py

import numpy as np
import rasterio
from rasterio.plot import reshape_as_image
import logging
import time
from typing import Optional


logger = logging.getLogger(__name__)

class ImageProcessor:
    def __init__(self, satelite: str):
        self.satelite = satelite

    def merge_bandas_tif(
        self,
        r: str,
        g: str,
        b: str,
        b08: str,
        aot: str,
        b01: str,
        b05: str,
        b06: str,
        b07: str,
        b09: str,
        b11: str,
        b12: str,
        b8a: str,
        pvi: str,
        scl: str,
        tci: str,
        wvp: str,
        output_path: str
        ) -> str | None:
        """
        Mescla várias bandas em um único GeoTIFF multibanda, ignorando bandas com shape diferente.

        Returns:
            str | None: Caminho do arquivo salvo ou None se houve erro
        """
        logger.info(f"Mesclando bandas multiespectrais para: {output_path}")

        band_paths = [
            r, g, b, b08, aot, b01, b05, b06,
            b07, b09, b11, b12, b8a, pvi, scl,
            tci, wvp
        ]

        band_arrays = []
        band_names_validas = []

        try:
            ref_shape = None

            for i, path in enumerate(band_paths):
                with rasterio.open(path) as src:
                    band = src.read(1).astype(float)
                    band[band == 0] = np.nan

                    if ref_shape is None:
                        ref_shape = band.shape
                        profile = src.profile

                    if band.shape != ref_shape:
                        logger.warning(f"Banda {path} ignorada por ter shape {band.shape}, diferente do shape de referência {ref_shape}")
                        continue

                    logger.warning(f"Banda {path} adicionada ao raster final")
                    band_arrays.append(band)
                    band_names_validas.append(path)
                    time.sleep(10)
                    

        except rasterio.errors.RasterioIOError as e:
            logger.error(f"Falha ao abrir uma das bandas:\n{e}")
            return None
        except Exception as e:
            logger.error(f"Erro inesperado ao ler bandas:\n{e}")
            return None

        if len(band_arrays) == 0:
            logger.error("Nenhuma banda válida foi encontrada para mesclagem.")
            return None

        def normalize_soft(array):
            array_min, array_max = np.nanmin(array), np.nanmax(array)
            if array_max - array_min == 0:
                return np.zeros_like(array, dtype=np.uint8)
            scaled = (array - array_min) / (array_max - array_min) * 255
            return np.nan_to_num(scaled, nan=0).astype(np.uint8)

        def normalize_percentile(array):
            p2, p98 = np.nanpercentile(array, (2, 98))
            array = np.clip(array, p2, p98)
            if np.nanmax(array) - np.nanmin(array) == 0:
                return np.zeros_like(array, dtype=np.uint8)
            scaled = (array - np.nanmin(array)) / (np.nanmax(array) - np.nanmin(array)) * 255
            return np.nan_to_num(scaled, nan=0).astype(np.uint8)

        norm_func = normalize_soft if self.satelite == 'S2-16D-2' else normalize_percentile

        try:
            profile.update(count=len(band_arrays), dtype=rasterio.uint8, driver="GTiff")

            with rasterio.open(output_path, 'w', **profile) as dst:
                for i, band in enumerate(band_arrays):
                    norm_band = norm_func(band)
                    norm_band[np.isnan(band)] = 0  # máscara individual por banda
                    dst.write(norm_band, i + 1)

            logger.info(f"Imagem multibanda salva com {len(band_arrays)} bandas em: {output_path}")
            return output_path

        except Exception as e:
            logger.error(f"Falha ao processar ou salvar imagem multibanda:\n{e}")
            return None