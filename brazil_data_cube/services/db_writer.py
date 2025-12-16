import asyncio
import logging
from datetime import date
from typing import Callable, Optional

from shapely.geometry import box
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from brazil_data_cube.api.models.models_db import SatelliteScene
from brazil_data_cube.config import DATABASE_URL
from brazil_data_cube.utils.get_tile_geometry import GeometryLoader
from brazil_data_cube.utils.exceptions import OrphanFileError


class DatabaseRecorder:
    def __init__(self,
                 logger: logging.Logger,
                 tile_paths: dict,
                 session_factory=None):
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
        band: str,
        upload_callback: Callable[[], None]
    ):
        """
        Método Síncrono chamado pelo Downloader.
        """

        # if "ZZZZZ" not in filename:
        #     raise ValueError("TESTANDO")

        # Resolve Geometria
        geometry = self.resolve_geometry(sat, tile_id, bbox)
        geom_wkt = str(geometry) if geometry else None

        # Dispara a tarefa async
        try:
            self.run_async(self.save_async(
                filename,
                mission,
                sat,
                tile_id,
                date_obj,
                minio_path,
                geom_wkt,
                band,
                upload_callback
            ))
        except Exception as e:
            self.logger.error(f"Erro crítico no DatabaseRecorder: {e}")
            raise e

    async def save_async(
        self,
        filename,
        mission,
        sat,
        tile_id,
        date_obj,
        minio_path,
        geom_wkt,
        band,
        upload_callback
    ):
        """
        Lógica de inserção isolada.
        """
        local_engine = create_async_engine(DATABASE_URL, echo=False)
        LocalSession = sessionmaker(
            bind=local_engine, class_=AsyncSession, expire_on_commit=False)

        file_uploaded = False
        async with LocalSession() as session:
            try:
                # 1. Prepara o objeto
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

                # 2. Adiciona e verifica constraints (Flush)
                session.add(new_scene)
                await session.flush() 
                
                # 3. Faz o Upload (Callback)
                # Se der erro aqui, pula pro except e file_uploaded continua False
                await asyncio.to_thread(upload_callback)
                file_uploaded = True 

                # 4. Commit Explícito
                # Se der erro aqui file_uploaded já é True
                await session.commit()
                
                self.logger.info(f"Transação concluída (DB+MinIO): {filename}")

            except Exception as e:
                # Garante que o banco volte ao estado anterior imediatamente
                await session.rollback()
                self.logger.error(f"Rollback executado para {filename}: {e}")

                # Lógica do Arquivo Órfão
                if file_uploaded:
                    self.logger.warning(f"Commit falhou após upload. Arquivo órfão: {minio_path}")
                    # Lança erro especial para o ScenePersister limpar
                    raise OrphanFileError(minio_path, e)
                
                # Se não foi órfão (erro no upload ou no flush), só repassa o erro original
                raise e
            
            finally:
                await local_engine.dispose()

    def resolve_geometry(self, sat: str, tile_id: str, bbox: list):
        """
        Lógica de tratamento de geometria
        """
        if (sat.upper().startswith("S1A") or "SENTINEL1" in sat.upper()) \
                and "_" in str(tile_id):
            return box(*bbox)

        shp_path = self.tile_paths.get(sat.upper())
        if not shp_path:
            return box(*bbox)

        try:
            loader = GeometryLoader(self.logger, shp_path)
            geom = loader.get_tile_geometry(tile_id, sat)
            if geom:
                return geom
        except Exception as e:
            self.logger.debug(
                f"Não foi possível carregar "
                f"geometria exata para {tile_id}: {e}")
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
            self.logger.error(
                f"Erro crítico ao disparar exclusão para {filename}: {e}")

    async def delete_async(self, filename: str):
        """
        Executa o DELETE no banco de forma assíncrona.
        Cria uma sessão local para evitar erros de loop/thread.
        """
        local_engine = create_async_engine(DATABASE_URL, echo=False)
        LocalSession = sessionmaker(
            bind=local_engine, class_=AsyncSession, expire_on_commit=False)

        try:
            async with LocalSession() as session:
                result = await session.execute(
                    delete(SatelliteScene).where(
                        SatelliteScene.filename == filename)
                )
                await session.commit()
                if result.rowcount > 0:
                    self.logger.warning(
                        f"Registro {filename} excluído "
                        f"do DB (Compensação MinIO).")
                else:
                    self.logger.warning(
                        f"Tentativa de exclusão falhou: "
                        f"{filename} não encontrado.")
        except Exception as e:
            self.logger.error(f"Erro SQL ao excluir {filename}: {e}")
        finally:
            await local_engine.dispose()


