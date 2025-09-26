import psycopg2

databases = [
    "postgresql://gen_user:Passwd123@81.200.145.49:5432/default_db",
    "postgresql://gen_user:Passwd123@176.57.220.211:5432/default_db",
    "postgresql://gen_user:Passwd123@45.153.71.20:5432/default_db",
    "postgresql://gen_user:Passwd123@89.223.64.236:5432/default_db",
]

for db_url in databases:
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()

    # Получение списка всех таблиц
    cur.execute("CREATE USER psql_create_user WITH PASSWORD 'psql1123fad';")

    # Завершение транзакции и закрытие соединения
    conn.commit()
    cur.close()
    conn.close()
