# brazil_data_cube/services/scene_persister.py

import os
import logging
from datetime import datetime
from typing import Optional
import time

from brazil_data_cube.minio.MinioUploader import MinioUploader
from brazil_data_cube.services.db_writer import DatabaseRecorder
from brazil_data_cube.utils.mission_info import MissionInfo
from brazil_data_cube.utils.exceptions import OrphanFileError


class ScenePersister:
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
        Processa um lote de arquivos baixados chamando a persistência
        individual para cada um.
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
        Executa a lógica transacional:
        1. Tenta salvar metadado no Banco.
        2. Tenta fazer upload no MinIO.
        3. Se MinIO falhar, remove do Banco (Rollback).
        """
        filename = os.path.basename(local_path)

        # Constrói o path final no MinIO
        object_name = os.path.join(
            mission_info.bucket_prefix, filename).replace("\\", "/")

        # Extrai nome da banda
        band_name = self.extract_band_name(filename)

        def upload_action():
            self.logger.info(f"Iniciando Upload MinIO para {filename}...")
            
            self.uploader.upload_and_cleanup_file(
                local_path, object_name=object_name
            )

        try:
            self.db_recorder.save_scene(
                filename=filename,
                mission=mission_info.mission,
                sat=mission_info.sat,
                tile_id=tile_id,
                date_obj=date_obj.date(),
                minio_path=object_name,
                bbox=bbox,
                band=band_name,
                upload_callback=upload_action # Passamos a função, não o resultado!
            )
            self.logger.info(f"Sucesso total (DB + MinIO) para {filename}")

        except OrphanFileError as orphan_e:
            self.logger.error(f"Tratando arquivo órfão: {orphan_e.minio_path}")
            try:
                self.uploader.client.remove_object(
                    self.uploader.bucket_name, 
                    orphan_e.minio_path
                )
                self.logger.info("Arquivo órfão removido com sucesso.")
            except Exception as del_err:
                self.logger.critical(f"ERRO FATAL: Falha ao remover órfão: {del_err}")

        except Exception as e:
            self.logger.error(f"Falha no processo de {filename}: {e}")

    def execute_upload_transaction(
            self,
            local_path: str,
            object_name: str,
            filename: str):
        """Bloco isolado para Upload com Rollback no DB em caso de falha."""
        try:
            self.logger.info(f"2/3. Upload MinIO para {filename}...")
            self.uploader.upload_and_cleanup_file(
                local_path, object_name=object_name)
            self.logger.info(f"3/3. Processo concluído para {filename}.")

        except Exception as minio_e:
            self.logger.error(
                f"3/3. Falha no Upload MinIO para {filename}: "
                f"{minio_e}. Executando compensação (DELETE DB).")
            try:
                self.db_recorder.delete_scene(filename)
            except Exception as delete_e:
                self.logger.critical(
                    f"ERRO FATAL: Falha ao excluir do "
                    f"DB após falha no MinIO: {delete_e}")

    def extract_band_name(self, filename: str) -> Optional[str]:
        try:
            return filename.split("_")[-1].split(".")[0]
        except Exception:
            return None

    def parse_date(self, date_str: str) -> datetime:
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
