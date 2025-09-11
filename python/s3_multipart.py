#!/usr/bin/env python3

import os
import sys
import time
import logging
import boto3
import botocore
import random
import string

# ================== НАСТРОЙКИ ==================
BUCKET_NAME = "my-test"
FILE_PATH = "/mnt/dbaas/tmp/test-bigfile.bin"
FILE_SIZE_GB =20  # можно менять 10-20
PART_SIZE_MB = 100  # размер части для multipart
REGION = "ru-1"
ENDPOINT_URL = "https://s3.twcstorage.ru"

# ================== ЛОГИ ==================
LOG_FILE = "s3_multipart.log"

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# формат логов
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")

# вывод в консоль
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)

# вывод в файл
file_handler = logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8")
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)

# добавляем обработчики
logger.addHandler(console_handler)
logger.addHandler(file_handler)

# включаем низкоуровневое логирование botocore (HTTP-запросы/ответы)
boto3.set_stream_logger('botocore', logging.DEBUG)

# ================== S3 клиент ==================
s3 = boto3.client(
    "s3",
    endpoint_url=ENDPOINT_URL,
    region_name=REGION,
    aws_access_key_id="YBKVN39ND675D1MK5K1C",
    aws_secret_access_key="oTuwuQ7mYJungSsXyDY2VQSMayOays48DjV43hTp",
)


def generate_file(path: str, size_gb: int):
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
    upload_id = None
    try:
        logger.info(f"Инициализация multipart upload для {key}")
        mpu = s3.create_multipart_upload(Bucket=bucket, Key=key)
        upload_id = mpu["UploadId"]
        logger.info(f"Создан multipart upload: UploadId={upload_id}")

        parts = []
        part_size = part_size_mb * 1024 * 1024
        total_size = os.path.getsize(file_path)
        total_parts = (total_size + part_size - 1) // part_size
        logger.info(f"Размер файла {total_size:,} байт, частей будет {total_parts}")

        with open(file_path, "rb") as f:
            part_number = 1
            while True:
                data = f.read(part_size)
                if not data:
                    break

                logger.info(f"Загрузка части {part_number}/{total_parts}, размер {len(data)} байт")
                response = s3.upload_part(
                    Bucket=bucket,
                    Key=key,
                    PartNumber=part_number,
                    UploadId=upload_id,
                    Body=data
                )
                etag = response["ETag"]
                logger.info(f"Часть {part_number} загружена, ETag={etag}")
                parts.append({"PartNumber": part_number, "ETag": etag})
                part_number += 1

        logger.info("Завершение multipart upload")
        result = s3.complete_multipart_upload(
            Bucket=bucket,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts}
        )
        logger.info(f"Загрузка завершена: {result}")

    except botocore.exceptions.ClientError as e:
        logger.error(f"ClientError при multipart upload: {e}")
        if upload_id:
            logger.warning(f"Прерывание multipart upload {upload_id}")
            s3.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
        raise
    except botocore.exceptions.EndpointConnectionError as e:
        logger.error(f"Ошибка подключения к endpoint: {e}")
        raise
    except Exception as e:
        logger.exception(f"Неожиданная ошибка при multipart upload: {e}")
        if upload_id:
            logger.warning(f"Прерывание multipart upload {upload_id}")
            s3.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
        raise


def main():
    generate_file(FILE_PATH, FILE_SIZE_GB)
    iteration = 1

    while True:
        try:
            object_key = f"test-bigfile-{iteration}-" + "".join(random.choices(string.ascii_lowercase + string.digits, k=6)) + ".bin"
            logger.info(f"=== Итерация {iteration}: загрузка {object_key} ===")

            multipart_upload(FILE_PATH, BUCKET_NAME, object_key, PART_SIZE_MB)

            logger.info("Удаление объекта из S3")
            s3.delete_object(Bucket=BUCKET_NAME, Key=object_key)
            logger.info(f"Объект {object_key} удален из S3")

            logger.info("Итерация завершена, пауза 5 секунд\n")
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