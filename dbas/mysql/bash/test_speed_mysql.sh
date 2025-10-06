#!/bin/bash

# Настройки подключения
MYSQL_HOST="81.19.135.52"
MYSQL_PORT=3306
MYSQL_USER="gen_user"
MYSQL_PASSWORD='Passwd123'
MYSQL_DB="default_db"

# Параметры теста
TABLES=30
TABLE_SIZE=1000000
THREADS=2
DURATION=300
SCRIPT="/usr/share/sysbench/oltp_read_write.lua"

# # Подготовка
echo $MYSQL_HOST
echo "🔧 Подготовка данных..."
sysbench $SCRIPT \
  --mysql-host=$MYSQL_HOST \
  --mysql-port=$MYSQL_PORT \
  --mysql-user=$MYSQL_USER \
  --mysql-password=$MYSQL_PASSWORD \
  --mysql-db=$MYSQL_DB \
  --tables=$TABLES \
  --table-size=$TABLE_SIZE \
  --threads=$THREADS \
  prepare

# Запуск теста
echo "🚀 Запуск теста..."
sysbench $SCRIPT \
  --mysql-host=$MYSQL_HOST \
  --mysql-port=$MYSQL_PORT \
  --mysql-user=$MYSQL_USER \
  --mysql-password=$MYSQL_PASSWORD \
  --mysql-db=$MYSQL_DB \
  --tables=$TABLES \
  --table-size=$TABLE_SIZE \
  --threads=$THREADS \
  --time=$DURATION \
  run

# Очистка
# echo "🧹 Очистка данных..."
# sysbench $SCRIPT \
#   --mysql-host=$MYSQL_HOST \
#   --mysql-port=$MYSQL_PORT \
#   --mysql-user=$MYSQL_USER \
#   --mysql-password=$MYSQL_PASSWORD \
#   --mysql-db=$MYSQL_DB \
#   --tables=$TABLES \
#   --table-size=$TABLE_SIZE \
#   --threads=$THREADS \
#   cleanup

echo "✅ Готово!"
