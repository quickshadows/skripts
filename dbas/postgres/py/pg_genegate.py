import csv
import random
from faker import Faker
from multiprocessing import Pool
import psycopg2
from tqdm import tqdm
import os

# Настройки
DB_CONN = "dbname=default_db user=gen_user password=JISDlsf!dn*5993) host=185.233.187.236"
CSV_DIR = "./csv_data"
os.makedirs(CSV_DIR, exist_ok=True)

N_USERS = 5_000_00
N_PRODUCTS = 500_00
N_ORDERS = 20_000_00
N_ORDER_ITEMS = 60_000_00
N_PAYMENTS = 20_000_00

N_PROCESSES = 8  # число процессов (по ядрам CPU)
CHUNK = 500_000  # сколько строк на один CSV-файл


# ---------- Генерация CSV ----------
def gen_users(start, end, filename):
    fake = Faker()
    with open(filename, "w", newline="") as f:
        writer = csv.writer(f)
        for i in range(start, end):
            writer.writerow([
                i+1,
                fake.name(),
                f"user{i}@example.com",   # гарант уникальность email
                fake.phone_number(),
                fake.address(),
                fake.date_time_this_decade()
            ])

def gen_products(start, end, filename):
    fake = Faker()
    with open(filename, "w", newline="") as f:
        writer = csv.writer(f)
        for i in range(start, end):
            writer.writerow([
                i+1,
                fake.word(),
                fake.word(),
                round(random.uniform(10, 5000), 2),
                fake.date_time_this_decade()
            ])

def gen_orders(start, end, filename):
    fake = Faker()
    with open(filename, "w", newline="") as f:
        writer = csv.writer(f)
        for i in range(start, end):
            writer.writerow([
                i+1,
                random.randint(1, N_USERS),
                fake.date_time_this_decade(),
                random.choice(["new","paid","shipped","cancelled"])
            ])

def gen_order_items(start, end, filename):
    with open(filename, "w", newline="") as f:
        writer = csv.writer(f)
        for i in range(start, end):
            writer.writerow([
                i+1,
                random.randint(1, N_ORDERS),
                random.randint(1, N_PRODUCTS),
                random.randint(1, 10),
                round(random.uniform(10, 5000), 2)
            ])

def gen_payments(start, end, filename):
    fake = Faker()
    with open(filename, "w", newline="") as f:
        writer = csv.writer(f)
        for i in range(start, end):
            writer.writerow([
                i+1,
                random.randint(1, N_ORDERS),
                round(random.uniform(10, 5000), 2),
                random.choice(["card","cash","paypal","crypto"]),
                fake.date_time_this_decade()
            ])


# ---------- Загрузка CSV в PostgreSQL ----------
def load_table(table, columns, filenames):
    conn = psycopg2.connect(DB_CONN)
    cur = conn.cursor()
    for file in filenames:
        with open(file, "r") as f:
            cur.copy_expert(f"COPY {table} ({','.join(columns)}) FROM STDIN WITH CSV", f)
        conn.commit()
    cur.close()
    conn.close()


# ---------- Основной процесс ----------
def run_generation(total, gen_func, prefix):
    tasks = []
    with Pool(N_PROCESSES) as p:
        for i in range(0, total, CHUNK):
            start = i
            end = min(i+CHUNK, total)
            filename = os.path.join(CSV_DIR, f"{prefix}_{i//CHUNK}.csv")
            tasks.append(p.apply_async(gen_func, (start, end, filename)))
        [t.get() for t in tqdm(tasks, desc=f"Generating {prefix}")]


def create_schema():
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
    conn = psycopg2.connect(DB_CONN)
    cur = conn.cursor()
    cur.execute(schema_sql)
    conn.commit()
    cur.close()
    conn.close()


if __name__ == "__main__":
    # 1. создаём схему
    create_schema()

    # 2. генерим CSV параллельно
    run_generation(N_USERS, gen_users, "users")
    run_generation(N_PRODUCTS, gen_products, "products")
    run_generation(N_ORDERS, gen_orders, "orders")
    run_generation(N_ORDER_ITEMS, gen_order_items, "order_items")
    run_generation(N_PAYMENTS, gen_payments, "payments")

    # 3. загружаем CSV в PostgreSQL
    load_table("users", ["id","full_name","email","phone","address","created_at"], [os.path.join(CSV_DIR, f) for f in os.listdir(CSV_DIR) if f.startswith("users_")])
    load_table("products", ["id","name","category","price","created_at"], [os.path.join(CSV_DIR, f) for f in os.listdir(CSV_DIR) if f.startswith("products_")])
    load_table("orders", ["id","user_id","created_at","status"], [os.path.join(CSV_DIR, f) for f in os.listdir(CSV_DIR) if f.startswith("orders_")])
    load_table("order_items", ["id","order_id","product_id","quantity","price"], [os.path.join(CSV_DIR, f) for f in os.listdir(CSV_DIR) if f.startswith("order_items_")])
    load_table("payments", ["id","order_id","amount","method","created_at"], [os.path.join(CSV_DIR, f) for f in os.listdir(CSV_DIR) if f.startswith("payments_")])

    print("✅ Данные сгенерированы и загружены в PostgreSQL!")
