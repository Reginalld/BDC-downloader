import asyncio
import logging
from datetime import date
from typing import Callable, Any

from shapely.geometry import box
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from brazil_data_cube.api.models.models_db import SatelliteScene
from brazil_data_cube.config import DATABASE_URL
from brazil_data_cube.utils.get_tile_geometry import GeometryLoader
from brazil_data_cube.utils.exceptions import OrphanFileError


class DatabaseRecorder:
    """
    Gerenciador de persistência relacional com suporte a transações distribuídas.

    Esta classe atua como uma fachada (Facade) para o SQLAlchemy, abstraindo a
    complexidade de sessões assíncronas (`asyncpg`).

    Seus dois principais diferenciais arquiteturais são:
    1. Bridge Sync/Async: O método `run_async` permite que códigos síncronos
       (como threads de download) chamem corrotinas de banco sem bloquear ou
       causar conflitos de Event Loop.
    2. Transação Estendida: O método `save_async` implementa um padrão de
       callback para garantir que o upload para o MinIO ocorra *dentro* da
       janela de validação do banco de dados (entre o Flush e o Commit).

    Attributes:
        logger (logging.Logger): Logger configurado.
        tile_paths (Dict[str, str]): Mapa de caminhos para Shapefiles de grade.
        global_factory (sessionmaker): Factory opcional (não usado no modo isolado).
    """
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
        Wrapper síncrono para persistência de uma cena.

        Prepara os dados (resolvendo a geometria) e despacha a execução para
        o loop de eventos assíncrono.

        Args:
            filename (str): Nome do arquivo físico.
            mission (str): Nome da missão.
            sat (str): Nome do satélite.
            tile_id (str): ID do Tile.
            date_obj (date): Data da cena.
            minio_path (str): Caminho relativo no Object Storage.
            bbox (list): Bounding Box [minx, miny, maxx, maxy].
            band (str): Nome da banda.
            upload_callback (Callable): Função sem argumentos que executa o upload.

        Raises:
            Exception: Propaga erros críticos ocorridos na thread async.
        """

        # if "ZZZZZ" not in filename:
        #     raise ValueError("TESTANDO")

        # Resolve Geometria (Tenta Shapefile exato, fallback para BBox)
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
        Executa a transação de persistência com injeção de I/O (Upload).

        Fluxo de Execução:
        1. Flush: Insere metadados no banco. O banco valida constraints
           (ex: chave única). Se falhar aqui, aborta antes do upload.
        2. Callback: Executa `upload_callback()` (MinIO Upload).
        3. Commit: Se o upload for sucesso, efetiva a gravação no banco.

        Tratamento de Falhas (Orphan Files):
        Se o passo 3 (Commit) falhar *após* o passo 2 ter ocorrido (ex: queda
        de conexão do DB), lança `OrphanFileError` para que o orquestrador
        apague o arquivo do MinIO.

        Args:
            (Mesmos argumentos de save_scene, mais geom_wkt processado)
        """
        # Cria engine local para garantir isolamento de thread
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

                # 2. Flush: Envia para o banco mas mantém transação aberta.
                # Isso dispara validações de integridade (Unique Constraints).
                session.add(new_scene)
                await session.flush()

                # 3. Callback de I/O (Upload para MinIO)
                # Executado em thread separada para não bloquear o loop async
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

                # Lógica Crítica: Arquivo Órfão
                # Se o arquivo subiu (file_uploaded=True) mas deu erro depois (no commit),
                # temos um arquivo no MinIO sem registro no Banco.
                if file_uploaded:
                    self.logger.warning(
                        f"Commit falhou após upload. "
                        f"Arquivo órfão: {minio_path}")
                    # Lança erro especial para o ScenePersister limpar
                    raise OrphanFileError(minio_path, e)

                # Se não foi órfão, só repassa o erro original
                raise e

            finally:
                await local_engine.dispose()

    def resolve_geometry(self, sat: str, tile_id: str, bbox: list):
        """
        Captura Geometria com base em IDs ou retorna Bbox normalalizado,
        que será adicionado na coluna geometry do banco.
        

        Tenta carregar o polígono exato do tile a partir do Shapefile de grade.
        Se falhar ou for satélite dinâmico (S1A), retorna o BBox retangular.

        Args:
            sat (str): Satélite.
            tile_id (str): ID do Tile.
            bbox (list): [minx, miny, maxx, maxy].

        Returns:
            Shapely Geometry (Polygon ou Box).
        """
        # Sentinel-1 (Radar) tem footprint dinâmico, não segue grade estática.
        # Retorna o BBox extraído da imagem.
        if (sat.upper().startswith("S1A") or "SENTINEL1" in sat.upper()) \
                and "_" in str(tile_id):
            return box(*bbox)

        # Busca caminho do shapefile na configuração
        shp_path = self.tile_paths.get(sat.upper())
        if not shp_path:
            return box(*bbox)

        try:
            # Tenta carregar geometria exata do arquivo de grade
            loader = GeometryLoader(self.logger, shp_path)
            geom = loader.get_tile_geometry(tile_id, sat)
            if geom:
                return geom
        except Exception as e:
            self.logger.debug(
                f"Não foi possível carregar "
                f"geometria exata para {tile_id}: {e}")
        return box(*bbox)

    def run_async(self, coroutine: Any) -> None:
        """
        Ponte de execução Síncrono -> Assíncrono (Event Loop Bridge).

        Detecta o contexto de execução atual:
        1. Se já existe um Loop rodando (ex: chamado via FastAPI), agenda uma Task.
        2. Se não existe Loop (ex: chamado via Thread Worker), cria um novo via `asyncio.run`.

        Args:
            coroutine (Coroutine): A função async a ser executada.
        """
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
