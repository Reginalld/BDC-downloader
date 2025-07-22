import logging
import os
import time
from pathlib import Path

from minio import Minio
from minio.error import S3Error

from brazil_data_cube.config import LOG_DIR


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
        self.logger = setup_minio_logger()

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
                if not self.client.bucket_exists(self.bucket_name):
                    self.client.make_bucket(self.bucket_name)
                    self.logger.info(f"Bucket criado: {self.bucket_name}")

                time.sleep(1)
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
                time.sleep(2 * attempt)

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

    def object_exists(self, object_name: str) -> bool:
        """
        Verifica se um objeto já existe no bucket.
        """
        try:
            self.client.stat_object(self.bucket_name, object_name)
            self.logger.info("[PULADO] Já existe no MinIO: %s", object_name)
            return True
        except S3Error as e:
            if e.code == "NoSuchKey":
                return False
            raise


def setup_minio_logger() -> logging.Logger:
    log_dir = Path(LOG_DIR)
    log_dir.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("minio_upload")
    logger.setLevel(logging.INFO)

    log_file = log_dir / "upload_minio.txt"

    if not logger.handlers:
        formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s"
        )
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

        logger.propagate = False

    return logger
