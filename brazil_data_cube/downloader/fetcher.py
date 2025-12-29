# brazil_data_cube/downloader/fetcher.py

import logging
from typing import Any, Dict, List, Optional, Tuple

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
    """
    Cliente especializado para busca e seleção de imagens na API STAC.

    Esta classe atua como um 'Broker' de metadados, abstraindo a complexidade
    de buscar e escolher a melhor imagem disponível. Ela implementa uma lógica
    de roteamento (Strategy Pattern implícito) para diferenciar os critérios
    de qualidade entre sensores Ópticos e de Radar.

    Attributes:
        logger (logging.Logger): Instância para registro de logs.
        connection (Any): Cliente `pystac_client`.
        resultmanager (ResultManager): Utilitário para auditoria de falhas.
    """
    def __init__(self, logger: logging.Logger, connection: any):
        self.connection = connection
        self.logger = logger
        self.resultmanager = ResultManager(logger)

    def fetch_image(self, satellite: str, bounding_box: list, start_date: str,
                    end_date: str, max_cloud_cover: float, tile_grid_path: str,
                    min_geometry_cover: float,
                    tile: Optional[str]) -> Optional[Dict[str, Any]]:
        """
        Orquestra a busca da melhor imagem aplicando filtros de negócio.

        O método decide qual estratégia de seleção utilizar:
        1. Radar (S1A): Delega para `select_best_radar_image`. Foca na
           interseção geométrica, ignorando nuvens.
        2. Óptico (S2, L8, CB): Delega para `select_best_optical_image`.
           Foca no menor percentual de nuvens e validação geométrica.

        Args:
            satellite (str): Identificador do satélite (ex: 'S2', 'S1A').
            bounding_box (List[float]): BBox [minx, miny, maxx, maxy].
            start_date (str): Data inicial 'YYYY-MM-DD'.
            end_date (str): Data final 'YYYY-MM-DD'.
            max_cloud_cover (float): Limite máximo de nuvens (0-100).
            tile_grid_path (str): Caminho do Shapefile de referência.
            min_geometry_cover (float): % mínima de cobertura do tile (0-100).
            tile (Optional[str]): ID do tile alvo (opcional).

        Returns:
            Optional[Dict[str, Any]]: O Item STAC selecionado ou None.
        """
        # Traduz 'S2' para 'S2_L2A-1', etc.
        collection_id = self.get_collection_id(satellite)

        self.logger.info(f"Buscando imagens do "
                         f"{satellite} ({collection_id})...")

        try:
            # 1. Busca inicial no STAC
            # Traz todos os candidatos no intervalo de tempo e espaço
            items = self.search_items(
                collection_id, bounding_box, start_date,
                end_date, max_cloud_cover
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
        """
        Resolve o ID técnico da coleção STAC.

        Args:
            satellite_name (str): Nome normalizado (ex: 'S2').

        Returns:
            str: ID da coleção (ex: 'S2_L2A-1').
        """
        for key, collection in COLLECTION_MAP.items():
            if key in satellite_name.upper():
                return collection
        return satellite_name

    def search_items(self, collection_id: str, bbox: list, start: str,
                     end: str, max_cloud: float) -> List[Any]:
        """
        Executa a consulta 'crua' na API do STAC.

        Nota:
            Embora alguns servidores aceitem filtro de nuvem na query,
            o servidor do BDC não apresenta funcionamento,
            (Sempre necessário testes para ver se passou a aceitar).

        Args:
            collection_id (str): Coleção alvo.
            bbox (List[float]): Área de interesse.
            start (str): Data ISO.
            end (str): Data ISO.
            max_cloud (float): Usado no filtro posterior.

        Returns:
            List[Any]: Lista de itens STAC brutos.
        """

        search_result = self.connection.search(
            bbox=bbox,
            datetime=[start, end],
            collections=[collection_id],
        )
        return list(search_result.items())

    def select_best_radar_image(
            self,
            items: List[Any],
            bounding_box: list) -> Optional[Dict]:
        """
        Estratégia de Seleção para Radar (Sentinel-1).

        Calcula a área de interseção entre o footprint da imagem e o BBox do usuário. # noqa: E501
        Seleciona a imagem que maximiza essa área (maior cobertura útil).

        Args:
            items (List[Any]): Lista de candidatos.
            bounding_box (List[float]): BBox alvo.

        Returns:
            Optional[Dict]: Melhor item segundo critério geométrico.
        """
        # Cria polígono do BBox desejado
        user_poly = box(*bounding_box)
        scored_items: List[Tuple[float, Any]] = []

        for item in items:
            try:
                # Extrai geometria real da imagem (que é torta/rotacionada)
                footprint_poly = self.extract_footprint(item)
                if not footprint_poly:
                    continue

                # Calcula % de cobertura: (Interseção / Área Desejada)
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
            f"Melhor imagem S1A encontrada com cobertura "
            f"geométrica de {best_coverage * 100:.2f}%"
        )
        return best_item

    def extract_footprint(self, item: Any) -> Optional[Any]:
        """
        Normaliza a extração de geometria do item STAC.

        Suporta tanto 'GeoFootprint' (GeoJSON) quanto 'Footprint' (WKT),
        com limpeza de prefixos SRID comuns em bancos PostGIS.

        Args:
            item (Any): Item STAC.

        Returns:
            Optional[Any]: Geometria Shapely pronta para cálculo.
        """
        props = item.properties
        if "GeoFootprint" in props:
            return shape(props["GeoFootprint"])
        elif "Footprint" in props:
            clean_wkt = props["Footprint"].replace("geography'SRID=4326;", "")
            return wkt.loads(clean_wkt)
        return None

    def select_best_optical_image(
            self,
            items: List[Any],
            tile: Optional[str],
            satellite: str,
            tile_grid_path: str,
            min_geo_cover: float,
            max_cloud_cover: float,
            start_date: str) -> Optional[Dict]:

        """
        Estratégia de Seleção para Ópticos (S2, L8, CBERS).

        Critérios de Aceite:
        1. Geometria: Deve cobrir no mínimo `min_geo_cover`% do tile.
        2. Nuvens: Deve ter menos que `max_cloud_cover`% de nuvens.
        3. Qualidade: Dentre as válidas, escolhe a com MENOR nuvem.

        Args:
            items (List[Any]): Candidatos.
            tile (Optional[str]): ID do Tile.
            satellite (str): Satélite.
            tile_grid_path (str): Caminho do Grid.
            min_geo_cover (float): Limite de geometria.
            max_cloud_cover (float): Limite de nuvem.
            start_date (str): Data para log.

        Returns:
            Optional[Dict]: Melhor item ou None.
        """
        # Instancia utilitário apenas se necessário
        geo_utils = GeometryUtils(self.logger, tile_grid_path)

        # Se tile não foi passado, tenta inferir do primeiro item
        target_tile = tile
        if not target_tile and items:
            bdc_tiles = items[0].properties.get('bdc:tiles', '')
            target_tile = bdc_tiles[0] if isinstance(bdc_tiles, list) \
                and bdc_tiles else ''

        # Filtra por geometria válida
        valid_items = [
            item for item in items
            if geo_utils.is_good_geometry(
                item, target_tile, satellite, min_geo_cover)
        ]

        if not valid_items:
            msg = "Imagem não passou no filtro de geometria." \
                if tile else "Nenhuma imagem disponível para os parâmetros."
            self.logger.warning(f"{msg} Tile: {tile}")
            if tile:
                self.resultmanager.log_error_csv(
                    tile, satellite, msg, start_date)
            return None

        # Ordena por cobertura de nuvem (menor para maior)
        valid_items.sort(
            key=lambda x: x.properties.get(CLOUD_PROP, float('inf')))
        best_item = valid_items[0]

        # Validação final de nuvens
        cloud_val = best_item.properties.get(CLOUD_PROP, float('inf'))

        if cloud_val > max_cloud_cover:
            self.logger.warning(
                "Nenhuma imagem que respeite o limite de nuvem foi encontrada")
            return None

        self.logger.info(f"Imagem selecionada com {cloud_val}% de nuvem.")
        return best_item

    def log_no_items(self, satellite, tile, date):
        """Log centralizado quando a busca retorna vazio."""
        if tile:
            msg = f"Nenhuma imagem disponível para o tile '{tile}'."
            self.logger.error(msg)
            self.resultmanager.log_error_csv(
                tile, satellite, "Nenhuma imagem encontrada.", date)
        elif "S1A" in satellite:
            self.logger.warning(
                "Nenhuma imagem S1A encontrada.")
        else:
            self.logger.warning(
                "Nenhuma imagem disponível para os parâmetros fornecidos.")

    def handle_error(
            self,
            e: Exception,
            satellite: str,
            tile: Optional[str],
            date: str):
        """Tratamento centralizado de exceções."""
        error_msg = str(e)
        self.logger.error(
            f"Erro ao obter imagem do {satellite}: {error_msg}", exc_info=True)
        self.resultmanager.log_error_csv(tile, satellite, error_msg, date)
