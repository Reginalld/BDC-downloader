import os
import time
from typing import Dict, Tuple

from minio import Minio
from minio.error import S3Error

from brazil_data_cube.utils.logger import ResultManager


class MinioUploader:
    def __init__(
            self, endpoint, access_key, secret_key, bucket_name, secure=False
            ):
        self.client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
        )
        self.bucket_name = bucket_name
        self.logger = ResultManager.setup_minio_logger()
        self.remover_log = ResultManager.setup_deletion_logger()

    def upload_file(self, local_path: str, object_name: str = None):
        """ Função que executa upload de um arquivo individual """
        if not object_name:
            object_name = os.path.basename(local_path)

        if not os.path.exists(local_path):
            self.logger.error(f"Arquivo não encontrado: {local_path}")
            return False

        file_size = os.path.getsize(local_path)
        self.logger.info(
            "Preparando para upload: %s (%.2f KB) -> %s/%s",
            local_path, file_size / 1024, self.bucket_name, object_name
        )

        attempt = 0
        max_retries = 3

        while attempt < max_retries:
            try:
                self.logger.debug(
                    "Tentativa %d de upload para: %s", attempt + 1, object_name
                )
                start_time = time.perf_counter()
                self.client.fput_object(
                    self.bucket_name, object_name, local_path
                    )
                duration = time.perf_counter() - start_time

                self.logger.info(
                    "Upload concluído: %s (%.2f KB) em %.2f segundos",
                    object_name, file_size / 1024, duration
                )
                return True
            except Exception as e:
                attempt += 1
                self.logger.warning(
                    "Tentativa %d falhou: %s -> %s: %s",
                    attempt, local_path, object_name, e
                )

        self.logger.error(
            "Falha ao fazer upload após %d tentativas: %s",
            max_retries, local_path
        )
        return False

    def upload_folder(self, folder_path: str, prefix: str = ""):
        """Faz upload recursivo de todos os arquivos de um diretório."""
        self.logger.info(
            "Iniciando upload recursivo da pasta: %s com prefixo '%s'",
            folder_path, prefix
        )
        files_uploaded = 0
        files_failed = 0

        for root, _, files in os.walk(folder_path):
            for file in files:
                local_file_path = os.path.join(root, file)
                relative_path = os.path.relpath(local_file_path, folder_path)
                object_name = os.path.join(prefix, relative_path) \
                                .replace("\\", "/")

                self.logger.debug(
                    "Processando arquivo para upload: %s -> %s",
                    local_file_path, object_name
                )
                success = self.upload_file(local_file_path, object_name)

                if success:
                    files_uploaded += 1
                else:
                    files_failed += 1

        self.logger.info(
            "Upload de pasta concluído: %d enviados com sucesso, %d falharam.",
            files_uploaded, files_failed
        )

    def object_exists(self, object_name: str, x: int) -> bool:
        """
        Verifica se um objeto já existe no bucket.
        """
        try:
            self.client.stat_object(self.bucket_name, object_name)

            if x == 0:
                self.logger.info(
                    "Arquivo pulado, pois já existe no MinIO: %s", object_name
                    )

            return True
        except S3Error as e:
            if e.code == "NoSuchKey":
                return False
            raise

    def upload_and_cleanup_file(
            self, local_path: str, object_name: str = None) -> bool:
        """
        Faz upload de um arquivo e remove o arquivo local
        caso o upload tenha sido bem-sucedido.
        Retorna True se arquivo enviado e removido
        com sucesso, False caso contrário.
        """
        if not object_name:
            object_name = os.path.basename(local_path)

        success = self.upload_file(local_path, object_name=object_name)
        if not success:
            self.logger.error(
                f"Upload falhou para {local_path}. Manter local.")
            return False

        # Confirma existência e remove
        try:
            if self.object_exists(object_name, x=1):
                try:
                    os.remove(local_path)
                    self.remover_log.info(
                        f"Arquivo removido após upload: {local_path}")
                except FileNotFoundError:
                    self.remover_log.warning(
                        f"Arquivo não encontrado "
                        f"ao tentar remover: {local_path}")
                except Exception as e:
                    self.remover_log.error(
                        f"Erro ao remover arquivo {local_path}: {e}")
                    return False
            else:
                # Se por algum motivo não foi encontrado após upload, logamos
                self.logger.warning(
                    f"Arquivo aparentemente não presente "
                    f"o bucket após upload: {object_name}")
                return False
        except Exception as e:
            self.logger.error(
                f"Erro ao verificar existência "
                f"em bucket para {object_name}: {e}")
            return False

        return True

    def upload_and_cleanup_batch(
            self,
            downloaded_files: Dict[str, str],
            prefix: str) -> Tuple[int, int]:
        """
        Faz upload de vários arquivos (um dict de paths),
        aplicando prefix e removendo os arquivos locais
        caso o upload tenha sido bem-sucedido.
        Retorna (num_success, num_failed).
        """
        num_success = 0
        num_failed = 0

        for local_path in downloaded_files.values():
            object_name = os.path.join(
                prefix, os.path.basename(local_path)).replace("\\", "/")

            try:
                success = self.upload_and_cleanup_file(
                    local_path, object_name=object_name)
            except Exception as e:
                self.logger.error(f"Erro ao processar {local_path}: {e}")
                success = False

            if success:
                num_success += 1
            else:
                num_failed += 1

        self.logger.info(
            "Batch upload finalizado: %d sucesso, %d falha (prefix=%s)",
            num_success, num_failed, prefix
        )
        return num_success, num_failed
