# brazil_data_cube/bounding_box_calculator.py

import math
import logging

logger = logging.getLogger(__name__)

class BoundingBoxCalculator:
    @staticmethod
    def calcular(lat, lon, raio_km):
        raio_graus_lat = raio_km / 111
        raio_graus_lon = raio_km / (111 * math.cos(math.radians(lat)))
        return [
            lon - raio_graus_lon,
            lat - raio_graus_lat,
            lon + raio_graus_lon,
            lat + raio_graus_lat,
        ]