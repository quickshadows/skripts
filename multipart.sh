#!/bin/bash

#set -e

# === Настройки ===
BUCKET="31d5eb06-test-upload"
KEY="large-upload.bin"
FILE="large-file.bin"
PART_SIZE_MB=128   # Размер одной части в MB (минимум 5)
REGION="ru-1"
ENDPOINT_URL="https://s3.twcstorage.ru"

# === Создание большого файла (1 ГБ) ===
if [ ! -f "$FILE" ]; then
  echo "Создаю файл размером 1 ГБ: $FILE"
  dd if=/dev/urandom of="$FILE" bs=128M count=8 status=progress
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
  dd if="$FILE" of="$PART_FILE" bs=128 skip=$OFFSET count=$PART_SIZE iflag=skip_bytes,count_bytes status=none

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

# Сохраним JSON в временный файл
TEMP_JSON_FILE=$(mktemp)
echo "$PARTS_JSON" > "$TEMP_JSON_FILE"

# Проверка корректности JSON
if ! jq . "$TEMP_JSON_FILE" > /dev/null; then
  echo "Ошибка: некорректный JSON для завершения загрузки."
  rm -f "$TEMP_JSON_FILE"
  exit 1
