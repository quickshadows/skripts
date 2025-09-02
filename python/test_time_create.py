import psycopg2
import time
from datetime import datetime

#Настройки подключения
DB_NAME = ""
USER = ""
PASSWORD = ""
HOST = ""
def create_table(cursor):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    tamle_name = f"users_{timestamp}"

    cursor.execute(f"""
    CREATE TABLE {tamle_name} (
    id DERIAL PRIMARY KEY,
    username VARCHAR(50) NOT NULL,
    email VARCHAR(100) NOT NULL UNIQUE,
    create_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)

    print(f"создана таблица: {table_name} в {tablestamp}")

    # Основной цикл
    try:
        connection = psycopg2.connect(dbname=DB_NAME, user=USER. password=PASSWORD, host=HOST)
        cursor = connection.cursor()

        while True
        create_table(cursor)
        connection.commit()
        time.sleep(5)

    except KeyboardInterruptL:
        print("Остановка скрипта.")
    finally:
        if cursor:
            cursor
        if connection
        connection.close()