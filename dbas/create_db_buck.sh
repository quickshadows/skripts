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

  # Проверка успешности получения плавающего IP
  if [ -z "$floating_ip" ]; then
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

  # Вывод результата создания базы данных
  echo "Ответ API на создание базы данных '$db_name': $response_database"
  echo " "
  echo " "
  sleep 2
}

# Получение плавающего IP
floating_ip=$(get_floating_ip)

# Создание баз данных для разных версий
# create_database "MySQL80 api buckup" "mysql" "519" "192.168.0.211"
# floating_ip=$(get_floating_ip)
# create_database "MySQL84 api buckup" "mysql8_4" "519" "192.168.0.212"
# floating_ip=$(get_floating_ip)

create_database "PostgreSQL 14 api buckup" "postgres14" "1175" "192.168.0.203"
floating_ip=$(get_floating_ip)
create_database "PostgreSQL 15 api buckup" "postgres15" "1175" "192.168.0.206"
floating_ip=$(get_floating_ip)
create_database "PostgreSQL 16 api buckup" "postgres16" "1175" "192.168.0.204"
floating_ip=$(get_floating_ip)
create_database "PostgreSQL 17 api buckup" "postgres17" "1175" "192.168.0.205"
# floating_ip=$(get_floating_ip)


