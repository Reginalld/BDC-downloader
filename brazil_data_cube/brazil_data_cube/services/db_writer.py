import logging
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession
from shapely.geometry import Polygon, box
from shapely.ops import transform

from brazil_data_cube.api.models.models_db import SatelliteScene
from brazil_data_cube.brazil_data_cube.utils.get_tile_geometry import GeometryLoader


class DatabaseRecorder:
    def __init__(self, logger: logging.Logger, session_factory, tile_paths: dict):
        """
        tile_paths = {
            "sentinel2": TILES_PATH_BDC_MD_V2,
            "landsat": TILES_PATH_LANDSAT,
            "sentinel1": TILES_PATH_SENTINEL,
        }
        """
        self.logger = logger
        self.session_factory = session_factory
        self.tile_paths = tile_paths

    def save_scene(
        self,
        filename: str,
        mission: str,
        sat: str,
        tile_id: str,
        date: datetime,
        minio_path: str,
        bbox: list,
    ):
        """
        Salva um registro no banco.
        """

        # 1. Determinar a geometria baseada no tile_id ou no bbox
        geometry = self._resolve_geometry(sat, tile_id, bbox)

        with self.session_factory() as session:
            scene = SatelliteScene(
                filename=filename,
                mission=mission,
                tile_id=tile_id,
                date=date,
                minio_path=minio_path,
                geometry=geometry
            )

            session.add(scene)
            session.commit()

            self.logger.info(f"Registro salvo no DB: {filename}")

    async def _resolve_geometry(self, sat: str, tile_id: str, bbox: list):
        # Sentinel-1 com lat/lon → usa bbox
        if sat.lower().startswith("s1") and "_" in tile_id:
            return box(*bbox)

        shp_path = self.tile_paths.get(sat.lower())
        if not shp_path:
            self.logger.warning(f"Nenhum SHP definido para {sat}. Usando bbox.")
            return box(*bbox)

        loader = GeometryLoader(self.logger, shp_path)
        geom = loader.get_tile_geometry(tile_id, sat)

        if geom:
            return geom

        # fallback
        return box(*bbox)
