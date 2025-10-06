#!/bin/bash

# Настройки подключения
PGSQL_HOST="192.168.0.31"
PGSQL_PORT=5432
PGSQL_USER="gen_user"
PGSQL_PASSWORD='3(8XQj88HQ7RS845R'
PGSQL_DB="default_db"

# Параметры теста
TABLES=8
TABLE_SIZE=10000
THREADS=8
DURATION=60
SCRIPT="/usr/share/sysbench/oltp_read_write.lua"

# Подготовка
echo $PGSQL_HOST
echo "🔧 Подготовка данных..."
sysbench --db-driver=pgsql $SCRIPT \
  --pgsql-host=$PGSQL_HOST \
  --pgsql-port=$PGSQL_PORT \
  --pgsql-user=$PGSQL_USER \
  --pgsql-password=$PGSQL_PASSWORD \
  --pgsql-db=$PGSQL_DB \
  --tables=$TABLES \
  --threads=$THREADS \
  --table-size=$TABLE_SIZE \
  prepare

# Запуск теста
echo "🚀 Запуск теста..."
sysbench --db-driver=pgsql $SCRIPT \
  --pgsql-host=$PGSQL_HOST \
  --pgsql-port=$PGSQL_PORT \
  --pgsql-user=$PGSQL_USER \
  --pgsql-password=$PGSQL_PASSWORD \
  --pgsql-db=$PGSQL_DB \
  --tables=$TABLES \
  --table-size=$TABLE_SIZE \
  --threads=$THREADS \
  --time=$DURATION \
  run

# Очистка
echo "🧹 Очистка данных..."
sysbench --db-driver=pgsql $SCRIPT \
  --pgsql-host=$PGSQL_HOST \
  --pgsql-port=$PGSQL_PORT \
  --pgsql-user=$PGSQL_USER \
  --pgsql-password=$PGSQL_PASSWORD \
  --pgsql-db=$PGSQL_DB \
  --tables=$TABLES \
  --table-size=$TABLE_SIZE \
  --threads=$THREADS \
  cleanup

echo "✅ Готово!"
