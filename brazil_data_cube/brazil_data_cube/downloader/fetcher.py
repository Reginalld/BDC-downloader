# brazil_data_cube/downloader/fetcher.py

import logging
from ..utils.logger import ResultManager
from typing import Optional, Dict, Any
from ..utils.geometry_utils import GeometryUtils



logger = logging.getLogger(__name__)

class SatelliteImageFetcher:
    def __init__(self, connection: any):
        self.connection = connection

    def fetch_image(self, satelite: str, bounding_box: list, start_date: str,
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
            logger.info(f"Buscando imagens do {satelite}...")

            # Construindo filtro com base no satélite
            filt = self._build_filter(satelite, max_cloud_cover)

            # Executa a busca na API com os parâmetros fornecidos
            search_result = self.connection.search(
                bbox=bounding_box,
                datetime=[start_date, end_date],
                collections=[satelite],
                filter=filt # Filtro não funcional no Stac utilizado pelo BDC, mas funcional em Stacs mais recentes
            )

            items = list(search_result.items()) # Converte resultados para lista

            if tile:
                if not items:
                    logger.error(f"Nenhuma imagem disponível para o tile '{tile}'.")
                    ResultManager().log_error_csv(tile, satelite, "Nenhuma imagem encontrada.",start_date)
                    return None

                geometry_utils = GeometryUtils(tile_grid_path)  # Instancia utilitário de geometria
                # Filtra imagens que cobrem adequadamente o tile
                items = [item for item in items if geometry_utils.is_good_geometry(item, tile, satelite)]

                if not items:
                    logger.warning(f"Nenhuma imagem passou no filtro de geometria para o tile: {tile}")
                    ResultManager().log_error_csv(tile, satelite, "Imagem não passou no filtro de geometria.",start_date)
                    return None
            else:
                if not items:
                    logger.warning("Nenhuma imagem disponível para os parâmetros fornecidos.")
                    return None

                # Tenta extrair o ID do tile usando os properties do BDC da primeira imagem
                tile = items[0].properties.get('tileId', '')
                geometry_utils = GeometryUtils(tile_grid_path)
                # Mesmo sem o tile informado, tenta validar a geometria da imagem com base no tile inferido
                items = [item for item in items if geometry_utils.is_good_geometry(item, tile, satelite)]

            # Seleciona a melhor imagem (menor cobertura de nuvem)
            items.sort(key=lambda item: item.properties.get('eo:cloud_cover', float('inf')))
            best_item = items[0]

            cloud_cover = best_item.properties.get('eo:cloud_cover', 'desconhecido')
            logger.info(f"Imagem selecionada com {cloud_cover}% de nuvem.")

            return best_item.assets  # Retorna os assets da imagem selecionada

        except Exception as e:
            erro_msg = str(e)
            logger.error(f"Erro ao obter imagem do {satelite}: {erro_msg}", exc_info=True)
            ResultManager().log_error_csv(tile, satelite, erro_msg,start_date)
            return None

    def _build_filter(self, satelite, max_cloud_cover):
        """Cria o filtro de busca com base no satélite(não funcional na API do Brazil Data Cube)."""
        if satelite == 'S2_L2A-1':
            # Aplica filtro com faixa de cobertura de nuvem entre 10 e o valor máximo permitido
            return {
                "op": "and",
                "args": [
                    {"op": "lte", "args": [{"property": "eo:cloud_cover"}, max_cloud_cover]},
                    {"op": "gte", "args": [{"property": "eo:cloud_cover"}, 10]},
                ],
            }
        elif satelite == 'S2-16D-2':
            # Apenas filtra imagens com cobertura de nuvem menor que o máximo permitido
            return {"op": "lte", "args": [{"property": "eo:cloud_cover"}, max_cloud_cover]}
        elif satelite == 'landsat-2':
            return {}
        else:
            raise ValueError(f"Satélite '{satelite}' não suportado.")