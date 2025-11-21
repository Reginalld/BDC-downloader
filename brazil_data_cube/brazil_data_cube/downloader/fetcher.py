# brazil_data_cube/downloader/fetcher.py

import logging
from typing import Any, Dict, Optional, Tuple, List

from shapely import wkt
from shapely.geometry import box, shape

from ..utils.geometry_utils import GeometryUtils
from ..utils.logger import ResultManager


CLOUD_PROP = "eo:cloud_cover"
COLLECTION_MAP = {
        "S2": "S2_L2A-1",
        "S1A": "sentinel-1-grd-bundle-1",
        "L8":  "landsat-2",
        "CB": "CBERS4-MUX-2M-1"
    }

class SatelliteImageFetcher:
    def __init__(self, logger: logging.Logger, connection: any):
        self.connection = connection
        self.logger = logger
        self.resultmanager = ResultManager(logger)

    def fetch_image(self, satellite: str, bounding_box: list, start_date: str,
                    end_date: str, max_cloud_cover: float, tile_grid_path: str,
                    min_geometry_cover: float,
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
        collection_id = self.get_collection_id(satellite)
        
        self.logger.info(f"Buscando imagens do {satellite} ({collection_id})...")

        try:
            # 1. Busca inicial no STAC
            items = self.search_items(
                collection_id, bounding_box, start_date, end_date, max_cloud_cover
            )

            if not items:
                self.log_no_items(satellite, tile, start_date)
                return None

            # 2. Roteamento de lógica: Radar (S1) vs Óptico (S2, L8, CBERS)
            if "S1A" in satellite:
                return self.select_best_radar_image(items, bounding_box)
            else:
                return self.select_best_optical_image(
                    items, tile, satellite, tile_grid_path, 
                    min_geometry_cover, max_cloud_cover, start_date
                )

        except Exception as e:
            self.handle_error(e, satellite, tile, start_date)
            return None

    def get_collection_id(self, satellite_name: str) -> str:
        """Resolve o ID da coleção baseado no nome do satélite."""
        for key, collection in COLLECTION_MAP.items():
            if key in satellite_name.upper():
                return collection
        return satellite_name 

    def search_items(self, collection_id: str, bbox: list, start: str, 
                      end: str, max_cloud: float) -> List[Any]:
        """Executa a busca crua na API STAC."""
        
        search_result = self.connection.search(
            bbox=bbox,
            datetime=[start, end],
            collections=[collection_id],
        )
        return list(search_result.items())

    def select_best_radar_image(self, items: List[Any], bounding_box: list) -> Optional[Dict]:
        """Lógica específica para Sentinel-1 (baseada em interseção geométrica)."""
        user_poly = box(*bounding_box)
        scored_items: List[Tuple[float, Any]] = []

        for item in items:
            try:
                footprint_poly = self.extract_footprint(item)
                if not footprint_poly:
                    continue

                # Calcula cobertura
                intersection = user_poly.intersection(footprint_poly).area
                coverage = intersection / user_poly.area
                scored_items.append((coverage, item))

            except Exception as e:
                self.logger.warning(f"Erro ao processar geometria S1A: {e}")
                continue

        if not scored_items:
            self.logger.warning("Nenhuma imagem S1A com geometria válida.")
            return None

        # Ordena pela maior cobertura
        scored_items.sort(key=lambda x: x[0], reverse=True)
        best_coverage, best_item = scored_items[0]

        self.logger.info(
            f"Melhor imagem S1A encontrada com cobertura geométrica de {best_coverage * 100:.2f}%"
        )
        return best_item

    def extract_footprint(self, item: Any) -> Optional[Any]:
        """Extrai geometria do item STAC (suporta GeoFootprint e WKT)."""
        props = item.properties
        if "GeoFootprint" in props:
            return shape(props["GeoFootprint"])
        elif "Footprint" in props:
            clean_wkt = props["Footprint"].replace("geography'SRID=4326;", "")
            return wkt.loads(clean_wkt)
        return None

    def select_best_optical_image(self, items: List[Any], tile: Optional[str], 
                                   satellite: str, tile_grid_path: str,
                                   min_geo_cover: float, max_cloud_cover: float,
                                   start_date: str) -> Optional[Dict]:
        """Lógica para satélites ópticos (filtro de nuvem e tile grid)."""
        
        # Instancia utilitário apenas se necessário
        geo_utils = GeometryUtils(self.logger, tile_grid_path)

        # Se tile não foi passado, tenta inferir do primeiro item (comportamento original)
        target_tile = tile
        if not target_tile and items:
            bdc_tiles = items[0].properties.get('bdc:tiles', '')
            target_tile = bdc_tiles[0] if isinstance(bdc_tiles, list) and bdc_tiles else ''

        # Filtra por geometria válida
        valid_items = [
            item for item in items
            if geo_utils.is_good_geometry(item, target_tile, satellite, min_geo_cover)
        ]

        if not valid_items:
            msg = "Imagem não passou no filtro de geometria." if tile else "Nenhuma imagem disponível para os parâmetros."
            self.logger.warning(f"{msg} Tile: {tile}")
            if tile:
                self.resultmanager.log_error_csv(tile, satellite, msg, start_date)
            return None

        # Ordena por cobertura de nuvem (menor para maior)
        valid_items.sort(key=lambda x: x.properties.get(CLOUD_PROP, float('inf')))
        best_item = valid_items[0]
        
        # Validação final de nuvens
        cloud_val = best_item.properties.get(CLOUD_PROP, float('inf'))
        
        if cloud_val > max_cloud_cover:
            self.logger.warning("Nenhuma imagem que respeite o limite de nuvem foi encontrada")
            return None

        self.logger.info(f"Imagem selecionada com {cloud_val}% de nuvem.")
        return best_item

    def log_no_items(self, satellite, tile, date):
        """Log centralizado quando a busca retorna vazio."""
        if tile:
            msg = f"Nenhuma imagem disponível para o tile '{tile}'."
            self.logger.error(msg)
            self.resultmanager.log_error_csv(tile, satellite, "Nenhuma imagem encontrada.", date)
        elif "S1A" in satellite:
            self.logger.warning("Nenhuma imagem S1A encontrada.")
        else:
            self.logger.warning("Nenhuma imagem disponível para os parâmetros fornecidos.")

    def handle_error(self, e: Exception, satellite: str, tile: Optional[str], date: str):
        """Tratamento centralizado de exceções."""
        error_msg = str(e)
        self.logger.error(f"Erro ao obter imagem do {satellite}: {error_msg}", exc_info=True)
        self.resultmanager.log_error_csv(tile, satellite, error_msg, date)