import logging
import asyncio
from datetime import date
from typing import Optional

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from shapely.geometry import box
from sqlalchemy import delete

from brazil_data_cube.api.models.models_db import SatelliteScene
from brazil_data_cube.utils.get_tile_geometry import GeometryLoader
from brazil_data_cube.config import DATABASE_URL

class DatabaseRecorder:
    def __init__(self, logger: logging.Logger, tile_paths: dict, session_factory=None):
        self.logger = logger
        self.tile_paths = tile_paths
        self.global_factory = session_factory

    def save_scene(
        self,
        filename: str,
        mission: str,
        sat: str,
        tile_id: str,
        date_obj: date,
        minio_path: str,
        bbox: list,
        band: str
    ):
        """
        Método Síncrono chamado pelo Downloader.
        """
        # Resolve Geometria
        geometry = self.resolve_geometry(sat, tile_id, bbox)
        geom_wkt = str(geometry) if geometry else None

        # Dispara a tarefa async
        try:
            self.run_async(self.save_async(
                filename, mission, sat, tile_id, date_obj, minio_path, geom_wkt, band
            ))
        except Exception as e:
            self.logger.error(f"Erro crítico no DatabaseRecorder: {e}")

    async def save_async(self, filename, mission, sat, tile_id, date_obj, minio_path, geom_wkt, band):
        """
        Lógica de inserção isolada.
        """
        local_engine = create_async_engine(DATABASE_URL, echo=False)
        LocalSession = sessionmaker(bind=local_engine, class_=AsyncSession, expire_on_commit=False)

        try:
            async with LocalSession() as session:
                try:
                    new_scene = SatelliteScene(
                        filename=filename,
                        satellite=sat,
                        mission=mission,
                        tile_id=tile_id,
                        date=date_obj,
                        band=band,
                        minio_path=minio_path,
                        geometry=geom_wkt
                    )
                    session.add(new_scene)
                    await session.commit()
                    self.logger.info(f"Salvo no DB: {filename}")
                except Exception as e:
                    await session.rollback()
                    if "unique constraint" in str(e).lower():
                        self.logger.info(f"Imagem duplicada (ignorado): {filename}")
                    else:
                        self.logger.error(f"Erro SQL: {e}")
        finally:
            # Importante: fecha a conexão local para não vazar
            await local_engine.dispose()

    def resolve_geometry(self, sat: str, tile_id: str, bbox: list):
        """
        Lógica de tratamento de geometria
        """
        if (sat.upper().startswith("S1A") or "SENTINEL1" in sat.upper()) and "_" in str(tile_id):
            return box(*bbox)
        
        shp_path = self.tile_paths.get(sat.upper())
        if not shp_path:
            return box(*bbox)
            
        try:
            loader = GeometryLoader(self.logger, shp_path)
            geom = loader.get_tile_geometry(tile_id, sat)
            if geom: return geom
        except:
            pass
        return box(*bbox)

    def run_async(self, coroutine):
        """Executa o loop de eventos."""
        try:
            # Tenta pegar loop existente
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop and loop.is_running():
            # Se já estamos num loop (ex: dentro da API direta), cria Task
            loop.create_task(coroutine)
        else:
            # Se estamos numa thread (Downloader), cria novo loop e roda
            asyncio.run(coroutine)

    def delete_scene(self, filename: str):
        """
        Método síncrono chamado para excluir um registro do banco.
        Usado como compensação (rollback) após falha no MinIO.
        """
        try:
            self.run_async(self.delete_async(filename))
        except Exception as e:
            self.logger.error(f"Erro crítico ao disparar exclusão para {filename}: {e}")

    async def delete_async(self, filename: str):
            """
            Executa o DELETE no banco de forma assíncrona.
            Cria uma sessão local para evitar erros de loop/thread.
            """
            local_engine = create_async_engine(DATABASE_URL, echo=False)
            LocalSession = sessionmaker(bind=local_engine, class_=AsyncSession, expire_on_commit=False)

            try:
                async with LocalSession() as session:
                    result = await session.execute(
                        delete(SatelliteScene).where(SatelliteScene.filename == filename)
                    )
                    await session.commit()
                    if result.rowcount > 0:
                        self.logger.warning(f"Registro {filename} excluído do DB (Compensação MinIO).")
                    else:
                        self.logger.warning(f"Tentativa de exclusão falhou: {filename} não encontrado.")
            except Exception as e:
                self.logger.error(f"Erro SQL ao excluir {filename}: {e}")
            finally:
                await local_engine.dispose()