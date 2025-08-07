#!/bin/bash

#set -e

# === Настройки ===
BUCKET="50c17271-multipart"
KEY="large-upload.bin"
FILE="large-file.bin"
PART_SIZE_MB=50   # Размер одной части в MB (минимум 5)
REGION="ru-1"
ENDPOINT_URL="https://s3.twcstorage.ru"

# === Создание большого файла (1 ГБ) ===
if [ ! -f "$FILE" ]; then
  echo "Создаю файл размером 1 ГБ: $FILE"
  dd if=/dev/urandom of="$FILE" bs=64M count=16 status=progress
fi

# === Инициализация мультипарт-загрузки ===
echo "Инициализация мультипарт-загрузки..."
UPLOAD_ID=$(aws s3api create-multipart-upload \
  --bucket "$BUCKET" \
  --key "$KEY" \
  --region "$REGION" \
  --endpoint-url "$ENDPOINT_URL" \
  --query UploadId \
  --output text)

echo "UploadId: $UPLOAD_ID"

# === Расчёт параметров ===
PART_SIZE=$((PART_SIZE_MB * 1024 * 1024))
FILE_SIZE=$(stat -c%s "$FILE")
PARTS_COUNT=$(( (FILE_SIZE + PART_SIZE - 1) / PART_SIZE ))

echo "Файл разбит на $PARTS_COUNT частей по $PART_SIZE_MB MB"

# === Временный каталог для частей ===
TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT

PARTS_JSON="["

# === Загрузка частей ===
for (( i=0; i<$PARTS_COUNT; i++ )); do
  OFFSET=$(( i * PART_SIZE ))
  PART_NUMBER=$(( i + 1 ))
  PART_FILE="$TMP_DIR/part-$PART_NUMBER"

  echo "Создаю часть $PART_NUMBER..."
  dd if="$FILE" of="$PART_FILE" bs=1 skip=$OFFSET count=$PART_SIZE iflag=skip_bytes,count_bytes status=none

  echo "Загружаю часть $PART_NUMBER..."
  RESPONSE=$(aws s3api upload-part \
    --bucket "$BUCKET" \
    --key "$KEY" \
    --part-number "$PART_NUMBER" \
    --body "$PART_FILE" \
    --upload-id "$UPLOAD_ID" \
    --region "$REGION" \
    --endpoint-url "$ENDPOINT_URL" \
    --output json)

  ETag=$(echo "$RESPONSE" | jq -r '.ETag')
  PARTS_JSON+="{\"PartNumber\":$PART_NUMBER,\"ETag\":$ETag}"

  if [ $PART_NUMBER -lt $PARTS_COUNT ]; then
    PARTS_JSON+=","
  fi
done

PARTS_JSON+="]"

# === Завершение мультипарт-загрузки ===
echo "Завершаю загрузку..."
aws s3api complete-multipart-upload \
  --bucket "$BUCKET" \
  --key "$KEY" \
  --upload-id "$UPLOAD_ID" \
  --multipart-upload "$PARTS_JSON" \
  --region "$REGION" \
  --endpoint-url "$ENDPOINT_URL"

echo "✅ Загрузка успешно завершена!"
