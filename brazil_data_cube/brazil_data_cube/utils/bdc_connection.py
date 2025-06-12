# brazil_data_cube/bdc_connection.py

import pystac_client
import logging

logger = logging.getLogger(__name__)

class BdcConnection:
    def __init__(self, endpoint: str ="https://data.inpe.br/bdc/stac/v1/"):
        self.endpoint = endpoint
        self.connection = None
        logger.info("BdcConnection inicializado.")

    def initialize(self):
        """
        Inicializa uma conexão com o Brazil Data Cube STAC API.
        
        Args:
            endpoint (str): URL do servidor STAC
        """
        try:
            logger.info("Conectando ao Brazil Data Cube...")
            self.connection = pystac_client.Client.open(self.endpoint)
            logger.info("Conexão com BDC estabelecida.")
        except Exception as e:
            logger.critical(f"Erro ao conectar ao BDC: {e}", exc_info=True)
            raise RuntimeError(f"Falha na inicialização do cliente BDC: {e}")

    def get_connection(self):
        """Retorna a conexão ativa, inicializando-a se necessário."""
        if self.connection is None:
            self.initialize()
        return self.connection