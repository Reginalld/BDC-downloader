# brazil_data_cube/downloader/fetcher.py

import logging
from typing import Any, Dict, Optional

from ..utils.geometry_utils import GeometryUtils
from ..utils.logger import ResultManager


class SatelliteImageFetcher:
    def __init__(self, logger: logging.Logger, connection: any):
        self.connection = connection
        self.logger = logger
        self.resultmanager = ResultManager(logger)

    def fetch_image(self, satellite: str, bounding_box: list, start_date: str,
                    end_date: str, max_cloud_cover: float, tile_grid_path: str,
                    tile: Optional[str]) -> Optional[Dict[str, Any]]:
        """
        Busca uma imagem usando filtro de nuvem e geometria.

        Args:
            satelite (str): Nome do satélite
            bounding_box (list): Coordenadas [minx, miny, maxx, maxy]
            start_date (str): Data início YYYY-MM-DD
            end_date (str): Data fim YYYY-MM-DD
            max_cloud_cover (float): Máximo de cobertura de nuvem (%)
            tile_grid_path (str): Caminho do shapefile de tiles
            tile (Optional[str]): ID do tile (opcional)

        Returns:
            Optional[Dict]: Assets da imagem ou None se não encontrar
        """
        try:
            self.logger.info(f"Buscando imagens do {satellite}...")

            # Construindo filtro com base no satélite
            stac_filter = self._build_filter(satellite, max_cloud_cover)

            # Executa a busca na API com os parâmetros fornecidos
            search_result = self.connection.search(
                bbox=bounding_box,
                datetime=[start_date, end_date],
                collections=[satellite],
                filter=stac_filter  # Filtro não funcional
                                    # no Stac utilizado pelo BDC
            )

            items = list(search_result.items())  # Converte resultados pra list

            if tile:
                if not items:
                    self.logger.error(
                        f"Nenhuma imagem disponível para o tile '{tile}'."
                        )
                    self.resultmanager.log_error_csv(
                        tile, satellite,
                        "Nenhuma imagem encontrada.", start_date
                        )
                    return None

                geometry_utils = GeometryUtils(
                    self.logger, tile_grid_path
                    )  # Instancia utilitário de geometria
                # Filtra imagens que cobrem adequadamente o tile
                items = [
                        item for item in items if
                        geometry_utils.is_good_geometry(item, tile, satellite)
                        ]

                if not items:
                    self.logger.warning(
                        f"Nenhuma imagem passou no filtro "
                        f"de geometria para o tile: {tile}"
                        )
                    self.resultmanager.log_error_csv(
                        tile,
                        satellite,
                        "Imagem não passou no filtro de geometria.",
                        start_date,
                    )
                    return None
            else:
                if not items:
                    self.logger.warning(
                        "Nenhuma imagem disponível "
                        "para os parâmetros fornecidos."
                    )
                    return None

                # Tenta extrair o ID do tile usando os
                # properties do BDC da primeira imagem
                if satellite == "S2_L2A-1":
                    tile = items[0].properties.get('tileId', '')
                else:
                    tile = items[0].properties.get('bdc:tiles', '')
                    tile = tile[0]

                geometry_utils = GeometryUtils(self.logger, tile_grid_path)
                # Mesmo sem o tile informado, tenta validar
                # a geometria da imagem com base no tile inferido
                items = [
                    item for item in items
                    if geometry_utils.is_good_geometry(item, tile, satellite)
                ]

            # Seleciona a melhor imagem (menor cobertura de nuvem)
            items.sort(
                key=lambda item: item.properties.get
                ('eo:cloud_cover', float('inf'))
            )

            best_item = items[0]

            cloud_cover = best_item.properties.get(
                'eo:cloud_cover', 'desconhecido'
                )
            self.logger.info(
                f"Imagem selecionada com {cloud_cover}% de nuvem."
            )

            return best_item.assets  # Retorna os assets da imagem selecionada

        except Exception as e:
            error_msg = str(e)
            self.logger.error(
                f"Erro ao obter imagem do {satellite}: {error_msg}",
                exc_info=True
            )
            self.resultmanager.log_error_csv(
                tile, satellite, error_msg, start_date
                )
            return None

    def _build_filter(self, satellite, max_cloud_cover):
        """
        Cria o filtro de busca com base no satélite
        (não funcional na API do Brazil Data Cube).
        """
        cloud_property = "eo:cloud_cover"

        if satellite == 'S2_L2A-1':
            return {
                "op": "and",
                "args": [
                    {
                        "op": "lte",
                        "args": [
                                {"property": cloud_property},
                                max_cloud_cover
                                  ],
                    },
                    {
                        "op": "gte",
                        "args": [{"property": cloud_property}, 10],
                    },
                ],
            }
        elif satellite == 'S2-16D-2':
            return {
                "op": "lte",
                "args": [{"property": cloud_property}, max_cloud_cover],
            }
        elif satellite == 'landsat-2':
            return {}
        else:
            raise ValueError(f"Satélite '{satellite}' não suportado.")
