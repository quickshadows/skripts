#!/bin/bash

# Функция для получения плавающего IP
get_floating_ip() {
  response_floating_ip=$(curl -s -X POST \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer $TIMEWEB_CLOUD_TOKEN" \
    -d '{
      "is_ddos_guard": false,
      "availability_zone": "spb-3"
    }' \
    "https://api.timeweb.cloud/api/v1/floating-ips")

  floating_ip=$(echo "$response_floating_ip" | jq -r '.ip.ip')

  if [ -z "$floating_ip" ] || [ "$floating_ip" == "null" ]; then
    echo "Ошибка: Не удалось получить плавающий IP."
    echo "Ответ API: $response_floating_ip"
    exit 1
  fi

  echo "$floating_ip"
  sleep 1
}

# Функция для создания базы данных
create_database() {
  local db_name=$1
  local db_type=$2
  local preset_id=$3
  local local_ip=$4
  local floating_ip=$5

  response_database=$(curl -s -X POST \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer $TIMEWEB_CLOUD_TOKEN" \
    -d "{
      \"name\": \"$db_name\",
      \"type\": \"$db_type\",
      \"preset_id\": $preset_id,
      \"availability_zone\": \"spb-3\",
      \"hash_type\": \"caching_sha2\",
      \"project_id\": 103757,
      \"auto_backups\": {
        \"copy_count\": 1,
        \"creation_start_at\": \"2025-08-18T13:51:32.132Z\",
        \"interval\": \"month\",
        \"day_of_week\": 1
      },
      \"admin\": {
        \"password\": \"Passwd123\",
        \"for_all\": false
      },
      \"network\": {
        \"id\": \"network-3654798e575f4dd3b9ad3e9dec940ead\",
        \"floating_ip\": \"$floating_ip\",
        \"local_ip\": \"$local_ip\"
      }
    }" \
    "https://cloud-staging.kube.timeweb.net/api/v1/databases")

  echo "Ответ API на создание базы данных '$db_name':"
  echo "$response_database"
  echo " "
  sleep 2
}

# Функция для получения занятых IP-адресов
get_busy_ips() {
  response=$(curl -s -H "Authorization: Bearer $TIMEWEB_CLOUD_TOKEN" \
    "https://api.timeweb.cloud/api/v2/vpcs/network-3654798e575f4dd3b9ad3e9dec940ead")

  busy_ips=$(echo "$response" | jq -r '.vpc.busy_address[]')

  if [ -z "$busy_ips" ]; then
    echo "Ошибка: Не удалось получить занятые IP-адреса."
    exit 1
  fi

  echo "$busy_ips"
}

# Функция для нахождения первого свободного IP-адреса
find_first_free_ip() {
  busy_ips=$(get_busy_ips)
  local all_ips=()

  for i in $(seq 2 254); do  # исключаем .0 (сеть) и .255 (broadcast)
    all_ips+=("192.168.0.$i")
  done

  for ip in "${all_ips[@]}"; do
    if ! echo "$busy_ips" | grep -q "$ip"; then
      echo "$ip"
      return
    fi
  done

  echo ""
}

# --- Основная логика ---
databases=(
  "PostgreSQL 14 api backup-3 postgres14 1175"
  "PostgreSQL 15 api backup-3 postgres15 1175"
  "PostgreSQL 16 api backup-3 postgres16 1175"
  "PostgreSQL 17 api backup-3 postgres17 1175"
)

for db in "${databases[@]}"; do
  db_name=$(echo "$db" | awk '{print $1" "$2" "$3" $4}')
  db_type=$(echo "$db" | awk '{print $5}')
  preset_id=$(echo "$db" | awk '{print $6}')

  floating_ip=$(get_floating_ip)
  local_ip=$(find_first_free_ip)

  if [ -z "$local_ip" ]; then
    echo "Ошибка: нет свободного приватного IP для $db_name"
    exit 1
  fi

  echo "Создание БД $db_name ($db_type) с IP: floating=$floating_ip, local=$local_ip"
  create_database "$db_name" "$db_type" "$preset_id" "$local_ip" "$floating_ip"
done
