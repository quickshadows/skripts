import psycopg2

databases = [
    "postgresql://gen_user:Passwd123@89.223.64.124:5432/default_db",
    "postgresql://gen_user:Passwd123@217.25.89.213:5432/default_db",
    "postgresql://gen_user:Passwd123@85.92.110.29:5432/default_db",
    "postgresql://gen_user:Passwd123@217.25.89.89:5432/default_db",
    "postgresql://gen_user:Passwd123@81.200.144.241:5432/default_db",
    "postgresql://gen_user:Passwd123@217.25.89.88:5432/default_db",
    "postgresql://gen_user:Passwd123@81.200.144.8:5432/default_db",
]

for db_url in databases:
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()

    # Получение списка всех таблиц
    cur.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'public';")
    tables = cur.fetchall()

    print(f"Таблицы в базе данных {db_url}:")
    for table in tables:
        print(f" - {table[0]}")

    # Завершение транзакции и закрытие соединения
    conn.commit()
    cur.close()
    conn.close()
