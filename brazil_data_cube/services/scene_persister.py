# brazil_data_cube/services/scene_persister.py

import os
import logging
from datetime import datetime
from typing import Optional

from brazil_data_cube.minio.MinioUploader import MinioUploader
from brazil_data_cube.services.db_writer import DatabaseRecorder
from brazil_data_cube.utils.mission_info import MissionInfo
from brazil_data_cube.utils.exceptions import OrphanFileError


class ScenePersister:
    """
    Coordenador de persistência atômica distribuída.

    Esta classe gerencia a consistência entre o armazenamento de metadados (PostgreSQL)
    e o armazenamento de arquivos físicos (MinIO). Ela implementa uma estratégia de
    'Transação Estendida' onde o upload do arquivo é executado dentro do contexto
    de validação do banco de dados via callback.

    Attributes:
        logger (logging.Logger): Logger para auditoria de transações.
        db_recorder (DatabaseRecorder): Cliente de banco de dados.
        uploader (MinioUploader): Cliente de Object Storage.
    """
    def __init__(
        self,
        logger: logging.Logger,
        db_recorder: DatabaseRecorder,
        uploader: MinioUploader
    ):
        self.logger = logger
        self.db_recorder = db_recorder
        self.uploader = uploader

    def persist_batch(
        self,
        download_files: dict,
        mission_info: MissionInfo,
        tile_id: str,
        start_datetime_str: str,
        bbox: list
    ) -> None:
        """
        Processa a persistência de um lote de arquivos (bandas de uma cena).

        Centraliza o parsing de datas para evitar repetição e itera sobre
        os arquivos baixados, delegando a persistência individual.

        Args:
            download_files (Dict[str, str]): Mapa {banda: caminho_local}.
            mission_info (MissionInfo): Metadados da missão.
            tile_id (str): Identificador do tile.
            start_datetime_str (str): Data de aquisição (ISO 8601).
            bbox (List[float]): Bounding box da cena.
        """
        # Converte a data uma única vez para o lote
        dt_obj = self.parse_date(start_datetime_str)

        for local_path in download_files.values():
            self.persist_single_file(
                local_path=local_path,
                mission_info=mission_info,
                tile_id=tile_id,
                date_obj=dt_obj,
                bbox=bbox
            )

    def persist_single_file(
        self,
        local_path: str,
        mission_info: MissionInfo,
        tile_id: str,
        date_obj: datetime,
        bbox: list
    ) -> None:
        """
        Executa a transação atômica para um único arquivo.

        Utiliza o padrão 'Inversion of Control' (IoC) para o upload:
        1. Cria uma função `upload_action` (closure) que encapsula a lógica de upload.
        2. Passa essa função como callback para `db_recorder.save_scene`.
        3. O `db_recorder` decide o momento seguro de chamar o upload (após flush).

        Também gerencia o mecanismo de compensação (limpeza de órfãos) caso
        o commit do banco falhe após o upload ter ocorrido.

        Args:
            local_path (str): Caminho do arquivo no disco local.
            mission_info (MissionInfo): Configurações da missão.
            tile_id (str): ID do Tile.
            date_obj (datetime): Data da cena.
            bbox (List[float]): Geometria.
        """
        filename = os.path.basename(local_path)

        # Constrói o path final no MinIO
        object_name = os.path.join(
            mission_info.bucket_prefix, filename).replace("\\", "/")

        # Extrai nome da banda
        band_name = self.extract_band_name(filename)

        # Definição do Callback
        # Esta função NÃO é executada agora. Ela será passada para o DB Recorder.
        def upload_action():
            self.logger.info(f"Iniciando Upload MinIO para {filename}...")

            self.uploader.upload_and_cleanup_file(
                local_path, object_name=object_name
            )

        try:
            # Inicia a transação no Banco
            self.db_recorder.save_scene(
                filename=filename,
                mission=mission_info.mission,
                sat=mission_info.sat,
                tile_id=tile_id,
                date_obj=date_obj.date(),
                minio_path=object_name,
                bbox=bbox,
                band=band_name,
                upload_callback=upload_action  # Passamos a função
            )
            self.logger.info(f"Sucesso total (DB + MinIO) para {filename}")

        except OrphanFileError as orphan_e:
            # Captura erro específico indicando que Upload OK mas DB falhou.
            self.logger.error(f"Tratando arquivo órfão: {orphan_e.minio_path}")
            try:
                # Remove o arquivo do MinIO para manter consistência com o banco (rollback físico)
                self.uploader.client.remove_object(
                    self.uploader.bucket_name,
                    orphan_e.minio_path
                )
                self.logger.info("Arquivo órfão removido com sucesso.")
            except Exception as del_err:
                # Se falhar aqui, requer intervenção manual (Log Crítico)
                self.logger.critical(
                    f"ERRO FATAL: Falha ao remover órfão: {del_err}")

        except Exception as e:
            # Erros genéricos (ex: falha de rede antes do upload) apenas logamos
            self.logger.error(f"Falha no processo de {filename}: {e}")

    def extract_band_name(self, filename: str) -> Optional[str]:
        """
        Extrai o identificador da banda a partir do nome do arquivo padronizado.

        Exemplo: 'S2_L2A_T22KGA_20230615_B04.tif' -> 'B04'

        Args:
            filename (str): Nome do arquivo.

        Returns:
            Optional[str]: Nome da banda ou None se falhar o split.
        """
        try:
            return filename.split("_")[-1].split(".")[0]
        except Exception:
            return None

    def parse_date(self, date_str: str) -> datetime:
        """
        Converte string de data ISO 8601 para objeto datetime.

        Trata inconsistências comuns (como presença ou falta de 'Z').

        Args:
            date_str (str): Data em string.

        Returns:
            datetime: Objeto datetime (ou datetime.now() em caso de erro).
        """
        if not date_str:
            return datetime.now()
        try:
            # Tenta tratar o formato ISO com ou sem Z
            clean_date = date_str.replace("Z", "+00:00")
            return datetime.fromisoformat(clean_date)
        except ValueError:
            self.logger.warning(
                f"Formato de data desconhecido: {date_str}. Usando data atual."
                )
            return datetime.now()
