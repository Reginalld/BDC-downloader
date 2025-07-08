import os
import time
import csv
import logging
from minio import Minio
from minio.error import S3Error
from brazil_data_cube.config import LOG_DIR

logger = logging.getLogger("minio_upload")
logger.setLevel(logging.INFO)

log_file = LOG_DIR / "upload_minio.txt"
log_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler = logging.FileHandler(log_file)
file_handler.setFormatter(log_formatter)
logger.addHandler(file_handler)

class MinioUploader:
    def __init__(self, endpoint, access_key, secret_key,bucket_name, secure=False):
        self.client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
        )
        self.bucket_name = bucket_name
    
    def upload_file(self, local_path:str, object_name: str = None):
        """ Função que executa upload de um arquivo individual """
        if not object_name:
            object_name = os.path.basename(local_path)

        if not os.path.exists(local_path):
            logger.error(f"Arquivo não encontrado: {local_path}")
            return False
    
        file_size = os.path.getsize(local_path)
        logger.info(f"Preparando para upload: {local_path} ({file_size / 1024:.2f} KB) -> {self.bucket_name}/{object_name}")

        attempt = 0
        max_retries = 3

        while attempt < max_retries:
            try:

                if not self.client.bucket_exists(self.bucket_name):
                    self.client.make_bucket(self.bucket_name)
                    logger.info(f"Bucket criado: {self.bucket_name}")

                time.sleep(1)
                logger.debug(f"Tentativa {attempt + 1} de upload para: {object_name}")
                start_time = time.perf_counter()
                self.client.fput_object(self.bucket_name, object_name, local_path)
                duration = time.perf_counter() - start_time

                logger.info(f"Upload concluído: {object_name} ({file_size / 1024:.2f} KB) em {duration:.2f} segundos")
                return True
            except Exception as e:
                attempt += 1
                logger.warning(f"Tentativa {attempt} falhou: {local_path} -> {object_name}: {e}")
                time.sleep(2 * attempt)

        logger.error(f"Falha ao fazer upload após {max_retries} tentativas: {local_path}")
        return False


    def upload_folder(self, folder_path: str, prefix: str = ""):
        """Faz upload recursivo de todos os arquivos de um diretório."""
        logger.info(f"Iniciando upload recursivo da pasta: {folder_path} com prefixo '{prefix}'")
        arquivos_enviados = 0
        arquivos_falharam = 0

        for root, _, files in os.walk(folder_path):
            for file in files:
                local_file_path = os.path.join(root, file)
                relative_path = os.path.relpath(local_file_path, folder_path)
                object_name = os.path.join(prefix, relative_path).replace("\\", "/")

                logger.debug(f"Processando arquivo para upload: {local_file_path} -> {object_name}")
                success = self.upload_file(local_file_path, object_name)

                if success:
                    arquivos_enviados += 1
                else:
                    arquivos_falharam += 1

        logger.info(f"Upload de pasta concluído: {arquivos_enviados} enviados com sucesso, {arquivos_falharam} falharam.")