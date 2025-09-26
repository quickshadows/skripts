import psycopg2
import time
from datetime import datetime

# Настройки подключения
DB_NAME = ""
USER = ""
PASSWORD = ""
HOST = ""

# Функция для создания таблицы
def create_table(cursor):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    table_name = f"users_{timestamp}"

    cursor.execute(f"""
    CREATE TABLE {table_name} (
        id SERIAL PRIMARY KEY,
        username VARCHAR(50) NOT NULL,
        email VARCHAR(100) NOT NULL UNIQUE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)
    
    print(f"Создана таблица: {table_name} в {timestamp}")

# Основной цикл
try:
    connection = psycopg2.connect(dbname=DB_NAME, user=USER, password=PASSWORD, host=HOST)
    cursor = connection.cursor()

    while True:
        create_table(cursor)
        connection.commit()  # Подтверждение изменений
        time.sleep(5)  # Задержка в 5 секунд между созданием таблиц

except KeyboardInterrupt:
    print("Остановка скрипта.")
finally:
    if cursor:
        cursor.close()
    if connection:
        connection.close()
