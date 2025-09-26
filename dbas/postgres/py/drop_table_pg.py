import psycopg2


databases = [
    "postgresql://gen_user:Passwd123@89.223.71.12:5432/default_db",
    "postgresql://gen_user:Passwd123@89.223.71.12:5432/db_write",
    "postgresql://gen_user:Passwd123@89.223.71.12:5432/db1",
    "postgresql://gen_user:Passwd123@89.223.71.12:5432/db2",
    "postgresql://gen_user:Passwd123@89.223.71.12:5432/db3",
    "postgresql://gen_user:Passwd123@89.223.71.12:5432/db4",
    "postgresql://gen_user:Passwd123@89.223.71.12:5432/db5",
    "postgresql://gen_user:Passwd123@89.223.71.12:5432/db6",
    "postgresql://gen_user:Passwd123@89.223.71.12:5432/db7",
    "postgresql://gen_user:Passwd123@89.223.71.12:5432/db8",
    "postgresql://gen_user:Passwd123@89.223.71.12:5432/db9",
]

for db_url in databases:
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()

    # Получение списка всех таблиц
    cur.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'public';")
    tables = cur.fetchall()

    # Вывод списка таблиц
    print(f"Таблицы в базе данных {db_url}:")
    for table in tables:
        print(f" - {table[0]}")

    # Удаление всех таблиц
    for table in tables:
        cur.execute(f"DROP TABLE IF EXISTS {table[0]} CASCADE;")

    # Завершение транзакции и закрытие соединения
    conn.commit()
    cur.close()
    conn.close()
