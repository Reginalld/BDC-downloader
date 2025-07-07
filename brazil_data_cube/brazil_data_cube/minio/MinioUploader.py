# import os
# import time
# import csv
# import logging
# from minio import Minio
# from minio.error import S3Error
# from brazil_data_cube.config import LOG_DIR

# logger = logging.getLogger("minio_upload")
# logger.setLevel(logging.INFO)

# log_file = LOG_DIR / "upload_minio.txt"
# log_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
# file_handler = logging.FileHandler(log_file)
# file_handler.setFormatter(log_formatter)
# logger.addHandler(file_handler)

# class MinioUploader:
#     def __init__(self, endpoint, access_key, secret_key,bucket_name, secure=False):
#         self.client = Minio(
#             endpoint,
#             access_key=access_key,
#             secret_key=secret_key,
#             secure=secure,
#         )
#         self.bucket_name = bucket_name

#         if not self.client.bucket_exists(bucket_name):
#             self.client.make_bucket(bucket_name)
#             logger.info(f"Bucket criado: {bucket_name}")
    
#     def upload_file(self, local_path:str, object_name: str = None):
#         """ Função que executa upload de um arquivo individual """
#         if not object_name:
#             object_name = os.path.basename(local_path)

#         attempt = 0
#         max_retries = 3
#         while attempt < max_retries:
#             try:
#                 self.client.fput_object(self.bucket_name, object_name, local_path)
#                 msg = f"Uploaded {local_path} - {self.bucket_name}/{object_name}"
#                 logger.info(msg)
#                 return True
#             except Exception as e:
#                 attempt += 1
#                 msg = f"Tentativa {attempt} falhou para {local_path} - {object_name}: {e}"
#                 logger.warning(msg)
#                 time.sleep(2 * attempt)


#     def upload_folder(self, folder_path: str, prefix: str = ""):
#         """ Função que faz upload recursivo de todos os arquivos do diretório """
#         for root, _, files in os.walk(folder_path):
#             for file in files:
#                 local_file_path = os.path.join(root, file)
#                 # Mantém estrutura relativa
#                 relative_path = os.path.relpath(local_file_path, folder_path)
#                 object_name = os.path.join(prefix, relative_path).replace("\\","/")
#                 print()
#                 print(object_name)
#                 self.upload_file(local_file_path, object_name)