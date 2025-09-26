#!/usr/bin/env python3
import os
import sys
import time
import logging
import boto3
import random
import string
from botocore.exceptions import (
    ClientError,
    EndpointConnectionError,
    ConnectTimeoutError,
    ReadTimeoutError,
    NoCredentialsError,
    PartialCredentialsError,
    ProfileNotFound,
    ParamValidationError,
)
from concurrent.futures import ThreadPoolExecutor, as_completed

# ================== НАСТРОЙКИ ==================
BUCKET_NAME = "31d5eb06-baket-backup-test"
FILE_SIZE_MB = 100           # размер тестового файла
REGION = "ru-1"
ENDPOINT_URL = "https://s3.twcstorage.ru"
PARALLEL_FILES = 100         # сколько файлов грузим одновременно
TMP_DIR = "/mnt/dbaas/tmp"

# ================== ЛОГИ ==================
LOG_FILE_MAIN = "s3_put.log"
LOG_FILE_ERRORS = "s3_errors.log"
LOG_FILE_REQUESTS = "s3_requests.log"

formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")

# общий лог
logger = logging.getLogger("s3tester")
logger.setLevel(logging.DEBUG)
fh_main = logging.FileHandler(LOG_FILE_MAIN, mode="a", encoding="utf-8")
fh_main.setLevel(logging.DEBUG)
fh_main.setFormatter(formatter)
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.INFO)
ch.setFormatter(formatter)
logger.addHandler(fh_main)
logger.addHandler(ch)

# ошибки
error_logger = logging.getLogger("s3errors")
error_logger.setLevel(logging.ERROR)
fh_err = logging.FileHandler(LOG_FILE_ERRORS, mode="a", encoding="utf-8")
fh_err.setLevel(logging.ERROR)
fh_err.setFormatter(formatter)
error_logger.addHandler(fh_err)

# запросы boto3/botocore
req_logger = logging.getLogger("botocore")
req_logger.setLevel(logging.DEBUG)
fh_req = logging.FileHandler(LOG_FILE_REQUESTS, mode="a", encoding="utf-8")
fh_req.setLevel(logging.DEBUG)
fh_req.setFormatter(formatter)
req_logger.addHandler(fh_req)

# ================== S3 клиент ==================
s3 = boto3.client(
    "s3",
    endpoint_url=ENDPOINT_URL,
    region_name=REGION,
    aws_access_key_id="XA2UUWUEW2WKT3IIDZ1Z",
    aws_secret_access_key="InwYkp2JtmzW96NpMfdDZFqyQGSWfA8JsDDMG8N2",
)

# ================== ЛОГИРОВАНИЕ ОШИБОК ==================
def log_s3_error(key: str, e: Exception):
    """Подробное логирование всех ошибок S3"""
    if isinstance(e, ClientError):
        code = e.response["Error"].get("Code", "Unknown")
        msg = e.response["Error"].get("Message", str(e))
        error_logger.error(f"[{key}] ClientError: {code} - {msg}", exc_info=True)
    elif isinstance(e, EndpointConnectionError):
        error_logger.error(f"[{key}] EndpointConnectionError: {e}", exc_info=True)
    elif isinstance(e, ConnectTimeoutError):
        error_logger.error(f"[{key}] ConnectTimeoutError: {e}", exc_info=True)
    elif isinstance(e, ReadTimeoutError):
        error_logger.error(f"[{key}] ReadTimeoutError: {e}", exc_info=True)
    elif isinstance(e, NoCredentialsError):
        error_logger.error(f"[{key}] NoCredentialsError: ключи не найдены", exc_info=True)
    elif isinstance(e, PartialCredentialsError):
        error_logger.error(f"[{key}] PartialCredentialsError: ключи заданы не полностью", exc_info=True)
    elif isinstance(e, ProfileNotFound):
        error_logger.error(f"[{key}] ProfileNotFound: профиль AWS CLI не найден", exc_info=True)
    elif isinstance(e, ParamValidationError):
        error_logger.error(f"[{key}] ParamValidationError: неверный параметр запроса - {e}", exc_info=True)
    else:
        error_logger.error(f"[{key}] Unexpected error: {type(e).__name__} - {e}", exc_info=True)

# ================== ФУНКЦИИ ==================
def generate_file(path: str, size_mb: int):
    """Создаёт тестовый файл указанного размера (если нет)."""
    if os.path.exists(path):
        logger.info(f"Файл {path} уже существует, пропускаем создание")
        return

    size_bytes = size_mb * 1024 * 1024
    logger.info(f"Создание файла {path} размером {size_mb} МБ (~{size_bytes:,} байт)")
    with open(path, "wb") as f:
        chunk = os.urandom(1024 * 1024)  # 1 MB случайных данных
        written = 0
        while written < size_bytes:
            f.write(chunk)
            written += len(chunk)
    logger.info("Файл создан")

def put_upload(file_path: str, bucket: str, key: str):
    """Загрузка файла целиком через put_object."""
    try:
        logger.info(f"[{key}] Загрузка файла {file_path}")
        with open(file_path, "rb") as f:
            s3.put_object(Bucket=bucket, Key=key, Body=f)
        logger.info(f"[{key}] Файл загружен")

        logger.info(f"[{key}] Удаление объекта из S3")
        s3.delete_object(Bucket=bucket, Key=key)
        logger.info(f"[{key}] Объект удалён")
    except Exception as e:
        log_s3_error(key, e)
        raise

def upload_one_file(iteration: int, file_path: str, bucket: str, idx: int):
    """Загрузка одного файла с уникальным ключом."""
    object_key = f"test-file-{iteration}-{idx}-" + \
                 "".join(random.choices(string.ascii_lowercase + string.digits, k=6)) + ".bin"
    try:
        logger.info(f"[TASK {idx}] Начало загрузки {object_key}")
        put_upload(file_path, bucket, object_key)
        logger.info(f"[TASK {idx}] Задача завершена")
    except Exception as e:
        log_s3_error(object_key, e)
        raise

def main():
    test_file = os.path.join(TMP_DIR, f"testfile_{FILE_SIZE_MB}MB.bin")
    generate_file(test_file, FILE_SIZE_MB)

    iteration = 1
    while True:
        try:
            logger.info(f"=== Итерация {iteration}: параллельная загрузка {PARALLEL_FILES} файлов ===")
            with ThreadPoolExecutor(max_workers=PARALLEL_FILES) as executor:
                futures = [
                    executor.submit(upload_one_file, iteration, test_file, BUCKET_NAME, idx+1)
                    for idx in range(PARALLEL_FILES)
                ]
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        logger.error(f"Ошибка в задаче: {e}")
            logger.info(f"Итерация {iteration} завершена, пауза 5 секунд\n")
            iteration += 1
            time.sleep(5)
        except KeyboardInterrupt:
            logger.info("Остановка по Ctrl+C")
            break
        except Exception as e:
            logger.exception(f"Ошибка в итерации {iteration}: {e}")
            time.sleep(10)

if __name__ == "__main__":
    main()
