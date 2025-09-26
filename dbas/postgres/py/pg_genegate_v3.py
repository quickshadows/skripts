import csv
import random
from faker import Faker
from multiprocessing import Pool
import psycopg2
from tqdm import tqdm
import os

# ---------- Настройки ----------
DB_CONNS = [
    "dbname=default_db user=gen_user password=Passwd123 host=147.45.110.148",
    "dbname=default_db user=gen_user password=Passwd123 host=81.200.144.241",
]

CSV_DIR = "./csv_data"
os.makedirs(CSV_DIR, exist_ok=True)

N_USERS = 5_000_000_0
N_PRODUCTS = 500_000_0
N_ORDERS = 20_000_000_0
N_ORDER_ITEMS = 60_000_000_0
N_PAYMENTS = 20_000_000_0

CHUNK = 500_000  # строки на один файл/чанк


# ---------- Генерация и загрузка чанка ----------
def gen_users_chunk(start, end, filename):
    fake = Faker()
    with open(filename, "w", newline="") as f:
        writer = csv.writer(f)
        for i in range(start, end):
            writer.writerow([i+1, fake.name(), f"user{i}@example.com",
                             fake.phone_number(), fake.address(),
                             fake.date_time_this_decade()])


def load_chunk(db_conn, table, columns, filename):
    conn = psycopg2.connect(db_conn)
    cur = conn.cursor()
    with open(filename, "r") as f:
        cur.copy_expert(f"COPY {table} ({','.join(columns)}) FROM STDIN WITH CSV", f)
    conn.commit()
    cur.close()
    conn.close()


def process_chunk(args):
    """Генерация и загрузка одного чанка в несколько баз параллельно"""
    start, end, prefix = args
    filename = os.path.join(CSV_DIR, f"{prefix}_{start//CHUNK}.csv")

    # 1️⃣ Генерим CSV если нет
    if not os.path.exists(filename):
        if prefix == "users":
            gen_users_chunk(start, end, filename)
        # тут можно добавить gen_products_chunk, gen_orders_chunk и т.д.
        # для краткости оставлен только users

    # 2️⃣ Загружаем этот CSV параллельно во все базы
    for db in DB_CONNS:
        load_chunk(db, prefix, get_columns(prefix), filename)


def get_columns(table):
    if table == "users":
        return ["id","full_name","email","phone","address","created_at"]
    elif table == "products":
        return ["id","name","category","price","created_at"]
    elif table == "orders":
        return ["id","user_id","created_at","status"]
    elif table == "order_items":
        return ["id","order_id","product_id","quantity","price"]
    elif table == "payments":
        return ["id","order_id","amount","method","created_at"]


# ---------- Создание схемы ----------
def create_schema(db_conn):
    schema_sql = """
    DROP TABLE IF EXISTS order_items, payments, orders, products, users CASCADE;

    CREATE TABLE users (
        id INT PRIMARY KEY,
        full_name TEXT,
        email TEXT,
        phone TEXT,
        address TEXT,
        created_at TIMESTAMP
    );

    CREATE TABLE products (
        id INT PRIMARY KEY,
        name TEXT,
        category TEXT,
        price NUMERIC(10,2),
        created_at TIMESTAMP
    );

    CREATE TABLE orders (
        id INT PRIMARY KEY,
        user_id INT REFERENCES users(id),
        created_at TIMESTAMP,
        status TEXT
    );

    CREATE TABLE order_items (
        id INT PRIMARY KEY,
        order_id INT REFERENCES orders(id),
        product_id INT REFERENCES products(id),
        quantity INT,
        price NUMERIC(10,2)
    );

    CREATE TABLE payments (
        id INT PRIMARY KEY,
        order_id INT REFERENCES orders(id),
        amount NUMERIC(10,2),
        method TEXT,
        created_at TIMESTAMP
    );
    """
    conn = psycopg2.connect(db_conn)
    cur = conn.cursor()
    cur.execute(schema_sql)
    conn.commit()
    cur.close()
    conn.close()


if __name__ == "__main__":
    # создаём схемы во всех базах
    for db in DB_CONNS:
        create_schema(db)

    # формируем список чанков для users
    chunks = [(i, min(i+CHUNK, N_USERS), "users") for i in range(0, N_USERS, CHUNK)]

    # параллельная генерация и загрузка
    with Pool(processes=os.cpu_count()) as pool:
        list(tqdm(pool.imap_unordered(process_chunk, chunks), total=len(chunks)))

    print("✅ CSV сгенерированы и загружены во все базы")
