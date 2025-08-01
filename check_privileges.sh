#!/bin/bash
read -rp "Введите хост PostgreSQL: " PGHOST

if [[ -n "$TOKEN" ]]; then
  echo "TOKEN задан"
else
  read -rp "Введите token: " TOKEN
  export TOKEN="$TOKEN"
#  echo "TOKEN установлена в: $TOKEN"
fi

# Конфигурация подключения
PGUSER="gen_user"
PGUSER1="user"
PGPASSWORD="Passwd123"
PGPORT="5432"
PGDATABASE="default_db"
SCHEMA="public"
#достаем idшники
DB_ID=$(twc db list | egrep "$PGHOST" | awk '{print $1}')
INSTANCE_ID=$(twc db instance list $DB_ID | egrep "default_db" | awk '{print $1}')

#Создание пользователя user
twc db user create $DB_ID --login "$PGUSER1" --password "$PGPASSWORD" --privileges ""

#Массив id юзеров
readarray -t ID_USERS < <(twc db user ls "$DB_ID" | awk 'NR > 1 { print $1 }')

# Временный файл для SQL
TMP_SQL=$(mktemp)

# Установим пароль
export PGPASSWORD="$PGPASSWORD"

function test_query_from_gen_user() {
    local label="$1"
    local sql="$2"

    echo "$sql" > "$TMP_SQL"

    local output
    output=$(psql -U "$PGUSER" -h "$PGHOST" -p "$PGPORT" -d "$PGDATABASE" -f "$TMP_SQL" -v ON_ERROR_STOP=1 2>&1)

    if [[ $? -eq 0 ]]; then
        echo "gen_user [PASS] $label"
    else
        echo "gen_user [FAIL] $label"
        echo "--- ERROR ---"
        echo "$output"
    fi
}

function test_query_from_user() {
    local label="$1"
    local sql="$2"

    echo "$sql" > "$TMP_SQL"

    local output
    output=$(psql -U "$PGUSER1" -h "$PGHOST" -p "$PGPORT" -d "$PGDATABASE" -f "$TMP_SQL" -v ON_ERROR_STOP=1 2>&1)

    if [[ $? -eq 0 ]]; then
        echo "user [PASS] $label"
    else
        echo "user [FAIL] $label"
        echo "--- ERROR ---"
        echo "$output"
    fi
}

function on_prava() {
for ID_USER in "${ID_USERS[@]}"; do
curl "https://timeweb.cloud/api/v1/databases/${DB_ID}/admins/${ID_USER}" \
  -X 'PATCH' \
  -H 'content-type: application/json' \
  -H "authorization: Bearer ${TOKEN}" \
  --data-raw '{"privileges":["INSERT","UPDATE","DELETE","CREATE","TRUNCATE","REFERENCES","TRIGGER","TEMPORARY","CREATEDB"],"instance_id":'"${INSTANCE_ID}"',"for_all":false}'
  echo -e "\n✔ Пользователь $ID_USER обработан"
  done

  for ID_USER in "${ID_USERS[@]}"; do
curl "https://timeweb.cloud/api/v1/databases/${DB_ID}/admins/${ID_USER}" \
  -X 'PATCH' \
  -H 'content-type: application/json' \
  -H "authorization: Bearer ${TOKEN}" \
  --data-raw '{"privileges":["SELECT","INSERT","UPDATE","DELETE","CREATE","TRUNCATE","REFERENCES","TRIGGER","TEMPORARY","CREATEDB"],"instance_id":'"${INSTANCE_ID}"',"for_all":false}'
  echo -e "\n✔ Пользователь $ID_USER обработан"
done
  sleep 60
}

#сбрасываем права
for ID_USER in "${ID_USERS[@]}"; do
curl "https://timeweb.cloud/api/v1/databases/${DB_ID}/admins/${ID_USER}" \
  -X 'PATCH' \
  -H 'content-type: application/json' \
  -H "authorization: Bearer ${TOKEN}" \
  --data-raw '{"privileges":[],"instance_id":'"${INSTANCE_ID}"',"for_all":false}'
  echo -e "\n✔ Пользователь $ID_USER обработан"
#  echo "URL: https://timeweb.cloud/api/v1/databases/${DB_ID}/admins/${ID_USER}"

done

sleep 90

# CREATE
test_query_from_gen_user "CREATE" "CREATE TABLE tmp_created_table (id INT);"

# INSERT
test_query_from_user "INSERT" "INSERT INTO tmp_created_table (id) VALUES ('1');"

# SELECT
test_query_from_user "SELECT" "SELECT * FROM tmp_created_table LIMIT 1;"

# UPDATE
test_query_from_user "UPDATE" "UPDATE tmp_created_table SET id = '3' WHERE id = '1';"


# TRUNCATE
test_query_from_user "TRUNCATE" "TRUNCATE TABLE tmp_created_table;
INSERT INTO tmp_created_table (id) VALUES ('1');"


# REFERENCES

test_query_from_gen_user "REFERENCES" "
DROP TABLE IF EXISTS ref_target, ref_source;
CREATE TABLE ref_target (id INT PRIMARY KEY);
"

test_query_from_user "REFERENCES" "
CREATE TABLE ref_source (
    ref_id INT REFERENCES ref_target(id)
);
"

test_query_from_gen_user "REFERENCES" "
DROP TABLE IF EXISTS ref_target, ref_source;
"

# TRIGGER

test_query_from_gen_user "TRIGGER" "
DROP TABLE IF EXISTS trg_table;
CREATE TABLE trg_table (id SERIAL, val TEXT);
"

test_query_from_user "TRIGGER" "

CREATE OR REPLACE FUNCTION trg_fn() RETURNS trigger AS \$\$
BEGIN
    NEW.val := 'triggered';
    RETURN NEW;
END;
\$\$ LANGUAGE plpgsql;

CREATE TRIGGER trg_bi
BEFORE INSERT ON trg_table
FOR EACH ROW
EXECUTE FUNCTION trg_fn();

INSERT INTO trg_table (val) VALUES ('test');
"

test_query_from_gen_user "TRIGGER" "
DROP TABLE trg_table;
"

# TEMPORARY
test_query_from_user "TEMPORARY" "CREATE TEMP TABLE temp_test (id INT);"

# DELETE
test_query_from_user "DELETE" "DELETE FROM tmp_created_table WHERE id= '1';"

#DROP
test_query_from_gen_user "DROP" "DROP TABLE tmp_created_table;"


#Включаем все права
for ID_USER in "${ID_USERS[@]}"; do
curl "https://timeweb.cloud/api/v1/databases/${DB_ID}/admins/${ID_USER}" \
  -X 'PATCH' \
  -H 'content-type: application/json' \
  -H "authorization: Bearer ${TOKEN}" \
  --data-raw '{"privileges":["SELECT","INSERT","UPDATE","DELETE","CREATE","TRUNCATE","REFERENCES","TRIGGER","TEMPORARY","CREATEDB"],"instance_id":'"${INSTANCE_ID}"',"for_all":false}'
  echo -e "\n✔ Пользователь $ID_USER обработан"
#  echo "URL: https://timeweb.cloud/api/v1/databases/${DB_ID}/admins/${ID_USER}"

done

sleep 90

# CREATE
test_query_from_gen_user "CREATE" "CREATE TABLE tmp_created_table (id INT);"
on_prava

# INSERT
test_query_from_user "INSERT" "INSERT INTO tmp_created_table (id) VALUES ('1');"
on_prava

# SELECT
test_query_from_user "SELECT" "SELECT * FROM tmp_created_table LIMIT 1;"
on_prava

# UPDATE
test_query_from_user "UPDATE" "UPDATE tmp_created_table SET id = '3' WHERE id = '1';"
on_prava

# TRUNCATE
test_query_from_user "TRUNCATE" "TRUNCATE TABLE tmp_created_table;
INSERT INTO tmp_created_table (id) VALUES ('1');"
on_prava

# REFERENCES

test_query_from_gen_user "REFERENCES" "
DROP TABLE IF EXISTS ref_target, ref_source;
CREATE TABLE ref_target (id INT PRIMARY KEY);
"
on_prava

test_query_from_user "REFERENCES" "
CREATE TABLE ref_source (
    ref_id INT REFERENCES ref_target(id)
);
"
on_prava

test_query_from_gen_user "REFERENCES" "
DROP TABLE IF EXISTS ref_target, ref_source;
"
on_prava

# TRIGGER

test_query_from_gen_user "TRIGGER" "
DROP TABLE IF EXISTS trg_table;
CREATE TABLE trg_table (id SERIAL, val TEXT);
"
on_prava

test_query_from_user "TRIGGER" "

CREATE OR REPLACE FUNCTION trg_fn() RETURNS trigger AS \$\$
BEGIN
    NEW.val := 'triggered';
    RETURN NEW;
END;
\$\$ LANGUAGE plpgsql;

CREATE TRIGGER trg_bi
BEFORE INSERT ON trg_table
FOR EACH ROW
EXECUTE FUNCTION trg_fn();

INSERT INTO trg_table (val) VALUES ('test');
"
on_prava

test_query_from_gen_user "TRIGGER" "
DROP TABLE trg_table;
"
on_prava

# TEMPORARY
test_query_from_user "TEMPORARY" "CREATE TEMP TABLE temp_test (id INT);"
on_prava

# DELETE
test_query_from_user "DELETE" "DELETE FROM tmp_created_table WHERE id= '1';"
on_prava

#DROP
test_query_from_gen_user "DROP" "DROP TABLE tmp_created_table;"

# Очистка
rm "$TMP_SQL"
