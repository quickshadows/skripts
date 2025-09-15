#!/usr/bin/env python3

import os
import sys
import time
import logging
import boto3
import botocore
import random
import string
from botocore.exceptions import ClientError
from concurrent.futures import ThreadPoolExecutor, as_completed

# ================== НАСТРОЙКИ ==================
BUCKET_NAME = "31d5eb06-baket-backup-test"
FILE_PATH = "/mnt/dbaas/tmp/test-bigfile.bin"
FILE_SIZE_GB = 1          # можно менять
PART_SIZE_MB = 20        # размер части для multipart
REGION = "ru-1"
ENDPOINT_URL = "https://s3.twcstorage.ru"
PARALLEL_FILES = 10         # 🔹 Сколько файлов грузим одновременно (2–5)

# ================== ЛОГИ ==================
LOG_FILE_MAIN = "s3_multipart.log"
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
    aws_access_key_id="B2UW7PYZZ3CDL9LKCROQ",
    aws_secret_access_key="YdFn798ci9iwWxLCWzO62gaqrPBhbbAcUaCq7w7a",
)


def generate_file(path: str, size_gb: int):
    """Создаёт тестовый файл указанного размера (если нет)."""
    if os.path.exists(path):
        logger.info(f"Файл {path} уже существует, пропускаем создание")
        return

    size_bytes = size_gb * 1024 * 1024 * 1024
    logger.info(f"Создание файла {path} размером {size_gb} ГБ (~{size_bytes:,} байт)")
    with open(path, "wb") as f:
        chunk = os.urandom(1024 * 1024)  # 1 MB случайных данных
        written = 0
        while written < size_bytes:
            f.write(chunk)
            written += len(chunk)
            if written % (1024 * 1024 * 1024) == 0:
                logger.info(f"Создано {written // (1024*1024*1024)} ГБ")
    logger.info("Файл создан")


def multipart_upload(file_path: str, bucket: str, key: str, part_size_mb: int = 100):
    """Загрузка файла в S3 по multipart upload."""
    upload_id = None
    try:
        logger.info(f"[{key}] Инициализация multipart upload")
        mpu = s3.create_multipart_upload(Bucket=bucket, Key=key)
        upload_id = mpu["UploadId"]
        logger.info(f"[{key}] UploadId={upload_id}")

        parts = []
        part_size = part_size_mb * 1024 * 1024
        total_size = os.path.getsize(file_path)
        total_parts = (total_size + part_size - 1) // part_size
        logger.info(f"[{key}] Размер {total_size:,} байт, частей {total_parts}")

        with open(file_path, "rb") as f:
            part_number = 1
            while True:
                data = f.read(part_size)
                if not data:
                    break

                logger.info(f"[{key}] Загрузка части {part_number}/{total_parts}, {len(data)} байт")
                response = s3.upload_part(
                    Bucket=bucket,
                    Key=key,
                    PartNumber=part_number,
                    UploadId=upload_id,
                    Body=data
                )
                etag = response["ETag"]
                logger.info(f"[{key}] Часть {part_number} загружена, ETag={etag}")
                parts.append({"PartNumber": part_number, "ETag": etag})
                part_number += 1

        logger.info(f"[{key}] Завершение multipart upload")
        result = s3.complete_multipart_upload(
            Bucket=bucket,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts}
        )
        logger.info(f"[{key}] Загрузка завершена: {result}")

    except botocore.exceptions.ClientError as e:
        msg = f"S3 ClientError при multipart upload ({key}): {e}"
        logger.error(msg)
        error_logger.exception(msg)
        if upload_id:
            logger.warning(f"Прерывание multipart upload {upload_id}")
            s3.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
        raise
    except botocore.exceptions.EndpointConnectionError as e:
        msg = f"Ошибка подключения к endpoint при загрузке {key}: {e}"
        logger.error(msg)
        error_logger.exception(msg)
        raise
    except Exception as e:
        msg = f"Неожиданная ошибка при multipart upload ({key}): {e}"
        logger.exception(msg)
        error_logger.exception(msg)
        if upload_id:
            logger.warning(f"Прерывание multipart upload {upload_id}")
            s3.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
        raise


def upload_one_file(iteration: int, file_path: str, bucket: str, part_size_mb: int, idx: int):
    """Загрузка одного файла с уникальным ключом."""
    try:
        object_key = f"test-bigfile-{iteration}-{idx}-" + \
                     "".join(random.choices(string.ascii_lowercase + string.digits, k=6)) + ".bin"
        logger.info(f"[TASK {idx}] Начало загрузки {object_key}")

        multipart_upload(file_path, bucket, object_key, part_size_mb)

        logger.info(f"[TASK {idx}] Удаление объекта {object_key} из S3")
        s3.delete_object(Bucket=bucket, Key=object_key)
        logger.info(f"[TASK {idx}] Объект {object_key} удалён")

    except Exception as e:
        msg = f"Ошибка в итерации {iteration}, объект {object_key}: {e}"
        logger.exception(msg)
        error_logger.exception(msg)
        raise

def main():
    generate_file(FILE_PATH, FILE_SIZE_GB)
    iteration = 1

    while True:
        try:
            logger.info(f"=== Итерация {iteration}: параллельная загрузка {PARALLEL_FILES} файлов ===")

            with ThreadPoolExecutor(max_workers=PARALLEL_FILES) as executor:
                futures = [
                    executor.submit(upload_one_file, iteration, FILE_PATH, BUCKET_NAME, PART_SIZE_MB, idx+1)
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
