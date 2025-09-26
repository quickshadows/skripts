#!/usr/bin/env python3
"""
synth_prod_db.py

Синтетическая "prod-like" БД ~10-20GB, без создания схем (работает в default search_path).
Генерация параллельная (мультипроцесс), запись в CSV чанками и загрузка в Postgres через COPY.
"""

import os
import csv
import gzip
import random
import uuid
import math
from datetime import datetime, timedelta, timezone
from multiprocessing import Pool, cpu_count
from functools import partial

import numpy as np
import psycopg2
from faker import Faker
from tqdm import tqdm

# ----------------- НАСТРОЙКИ -----------------
# Подставь своё подключение сюда (или оставь строку подключения)
DB_CONN = "dbname=default_db user=gen_user password=JISDlsf!dn*5993) host=185.233.187.236"

TARGET_DB_SIZE_GB = 12           # целевой размер БД (примерно) — выставь 10..20
CSV_DIR = "./csv_prodlike"       # временная папка для CSV
os.makedirs(CSV_DIR, exist_ok=True)

N_PROCESSES = min(12, max(2, cpu_count()))  # количество параллельных процессов
CHUNK = 500_000                  # строки на CSV-файл (подбирай под RAM/IO)
USE_GZIP = False                 # если True — создаёт .csv.gz (COPY FROM PROGRAM нужен на сервере)
RANDOM_SEED = 42

# Базовые ориентиры (можно менять)
BASE_USERS = 2_000_000
BASE_PRODUCTS = 300_000
BASE_CATEGORIES = 3_000
BASE_WAREHOUSES = 50
BASE_ORDERS = 5_000_000
AVG_ITEMS_PER_ORDER = 3.0
BASE_REVIEWS = 2_000_000
BASE_SESSIONS = 8_000_000

# оценки размера
AVG_EVENT_BYTES = 900

# Даты
TODAY = datetime.now(timezone.utc)
START_DATE = TODAY - timedelta(days=365 * 4)

# Инициализация стохастики
random.seed(RANDOM_SEED)
np.random.seed(RANDOM_SEED)
fake = Faker()
Faker.seed(RANDOM_SEED)

# Мета
COUNTRIES = ["US","DE","FR","GB","PL","ES","IT","NL","SE","NO","CZ","SK","LT","LV","EE","RO","BG","UA","CA","AU","BR","JP","SG","AE","IN"]
CURRENCIES = ["USD","EUR","PLN","GBP"]
EVENT_TYPES = ["auth.login","auth.logout","cart.add","cart.remove","order.create","order.pay","order.cancel","shipment.create","page.view","search.query","profile.update","review.create"]
USER_TAGS = ["vip","new","churn_risk","newsletter","wholesale","b2b","b2c","loyalty_gold","loyalty_silver"]
PRODUCT_TAGS = ["sale","new","exclusive","eco","refurbished","bundle","limited"]
WAREHOUSE_TZS = ["UTC","Europe/Berlin","Europe/Warsaw","Europe/Paris","America/New_York","Asia/Singapore"]

# ----------------- SQL СХЕМА (без создания схемы) -----------------
# Все таблицы будут созданы в default schema (обычно public). DROP IF EXISTS чтобы можно было запускать повторно.
def build_schema_sql():
    partitions_orders = []
    partitions_events = []
    # создаём месячные партиции за 48 месяцев назад (пример)
    for i in range(48):
        from_dt = (TODAY - timedelta(days=30*(i+1))).strftime("%Y-%m-%d")
        to_dt = (TODAY - timedelta(days=30*i)).strftime("%Y-%m-%d")
        ym = (TODAY - timedelta(days=30*i)).strftime("%Y_%m")
        partitions_orders.append(f"DROP TABLE IF EXISTS orders_{ym} CASCADE; CREATE TABLE orders_{ym} PARTITION OF orders FOR VALUES FROM ('{from_dt}') TO ('{to_dt}');")
        partitions_events.append(f"DROP TABLE IF EXISTS events_{ym} CASCADE; CREATE TABLE events_{ym} PARTITION OF events FOR VALUES FROM ('{from_dt}') TO ('{to_dt}');")

    schema = f"""
-- Удаляем старые таблицы, создаём новые в default schema (public или другая, настроенная у пользователя)
DROP TABLE IF EXISTS order_items CASCADE;
DROP TABLE IF EXISTS payments CASCADE;
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS products CASCADE;
DROP TABLE IF EXISTS accounts CASCADE;
DROP TABLE IF EXISTS addresses CASCADE;
DROP TABLE IF EXISTS categories CASCADE;
DROP TABLE IF EXISTS warehouses CASCADE;
DROP TABLE IF EXISTS inventory CASCADE;
DROP TABLE IF EXISTS reviews CASCADE;
DROP TABLE IF EXISTS sessions CASCADE;
DROP TABLE IF EXISTS events CASCADE;

-- Типы
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'order_status') THEN
        CREATE TYPE order_status AS ENUM ('new','paid','processing','shipped','delivered','cancelled','returned');
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'payment_method') THEN
        CREATE TYPE payment_method AS ENUM ('card','cash','paypal','crypto','bank_transfer','apple_pay','google_pay');
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'payment_status') THEN
        CREATE TYPE payment_status AS ENUM ('pending','authorized','captured','refunded','failed');
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'address_type') THEN
        CREATE TYPE address_type AS ENUM ('billing','shipping','other');
    END IF;
END$$;

CREATE TABLE categories (
    id              BIGINT PRIMARY KEY,
    name            TEXT NOT NULL,
    parent_id       BIGINT,
    path            TEXT,
    created_at      timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE warehouses (
    id              BIGINT PRIMARY KEY,
    code            TEXT UNIQUE NOT NULL,
    name            TEXT NOT NULL,
    country         TEXT,
    tz              TEXT,
    latitude        DOUBLE PRECISION,
    longitude       DOUBLE PRECISION,
    created_at      timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE accounts (
    id              BIGINT PRIMARY KEY,
    uuid            UUID NOT NULL,
    full_name       TEXT NOT NULL,
    email           TEXT UNIQUE NOT NULL,
    phone           TEXT,
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      timestamptz NOT NULL,
    last_login      timestamptz,
    preferences     JSONB,
    tags            TEXT[]
);

CREATE TABLE addresses (
    id              BIGINT PRIMARY KEY,
    account_id      BIGINT REFERENCES accounts(id) ON DELETE CASCADE,
    type            address_type NOT NULL,
    country         TEXT,
    city            TEXT,
    postal_code     TEXT,
    street          TEXT,
    created_at      timestamptz NOT NULL,
    geo             POINT
);

CREATE TABLE products (
    id              BIGINT PRIMARY KEY,
    sku             TEXT UNIQUE NOT NULL,
    name            TEXT NOT NULL,
    category_id     BIGINT REFERENCES categories(id),
    price           NUMERIC(12,2) NOT NULL,
    currency        CHAR(3) NOT NULL,
    attributes      JSONB,
    tags            TEXT[],
    weight_grams    INTEGER,
    created_at      timestamptz NOT NULL
);

CREATE TABLE inventory (
    product_id      BIGINT REFERENCES products(id) ON DELETE CASCADE,
    warehouse_id    BIGINT REFERENCES warehouses(id) ON DELETE CASCADE,
    qty             INTEGER NOT NULL,
    reserved        INTEGER NOT NULL DEFAULT 0,
    updated_at      timestamptz NOT NULL,
    PRIMARY KEY (product_id, warehouse_id)
);

CREATE TABLE orders (
    id              BIGINT NOT NULL,
    account_id      BIGINT REFERENCES accounts(id),
    status          order_status NOT NULL,
    total_amount    NUMERIC(12,2) NOT NULL,
    currency        CHAR(3) NOT NULL,
    created_at      timestamptz NOT NULL,
    updated_at      timestamptz,
    metadata        JSONB,
    PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);

{os.linesep.join(partitions_orders)}

CREATE TABLE order_items (
    id              BIGINT PRIMARY KEY,
    order_id        BIGINT NOT NULL,
    order_created   timestamptz NOT NULL,
    product_id      BIGINT REFERENCES products(id),
    qty             INTEGER NOT NULL,
    unit_price      NUMERIC(12,2) NOT NULL,
    discount        NUMERIC(12,2) DEFAULT 0,
    subtotal        NUMERIC(12,2) GENERATED ALWAYS AS (qty * unit_price - discount) STORED,
    FOREIGN KEY (order_id, order_created) REFERENCES orders(id, created_at) ON DELETE CASCADE
);

CREATE TABLE payments (
    id              BIGINT PRIMARY KEY,
    order_id        BIGINT NOT NULL,
    order_created   timestamptz NOT NULL,
    amount          NUMERIC(12,2) NOT NULL,
    method          payment_method NOT NULL,
    status          payment_status NOT NULL,
    details         JSONB,
    paid_at         timestamptz
);

CREATE TABLE reviews (
    id              BIGINT PRIMARY KEY,
    product_id      BIGINT REFERENCES products(id) ON DELETE CASCADE,
    account_id      BIGINT REFERENCES accounts(id) ON DELETE SET NULL,
    rating          SMALLINT CHECK (rating BETWEEN 1 AND 5),
    title           TEXT,
    body            TEXT,
    helpful_count   INTEGER DEFAULT 0,
    created_at      timestamptz NOT NULL
);

CREATE TABLE sessions (
    id              UUID PRIMARY KEY,
    account_id      BIGINT REFERENCES accounts(id) ON DELETE SET NULL,
    ip              INET,
    user_agent      TEXT,
    started_at      timestamptz NOT NULL,
    ended_at        timestamptz
);

CREATE TABLE events (
    id              BIGSERIAL,
    account_id      BIGINT,
    type            TEXT NOT NULL,
    payload         JSONB NOT NULL,
    created_at      timestamptz NOT NULL,
    PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);

{os.linesep.join(partitions_events)}
"""
    return schema

# ----------------- Индексы (создаются после загрузки) -----------------
INDEXES_SQL = """
CREATE INDEX IF NOT EXISTS idx_categories_parent ON categories (parent_id);
CREATE INDEX IF NOT EXISTS idx_products_category ON products (category_id);
CREATE INDEX IF NOT EXISTS idx_products_created ON products (created_at);
CREATE INDEX IF NOT EXISTS products_tags_gin ON products USING gin (tags);
CREATE INDEX IF NOT EXISTS products_attr_gin ON products USING gin (attributes);

CREATE INDEX IF NOT EXISTS idx_inventory_warehouse ON inventory (warehouse_id);
CREATE INDEX IF NOT EXISTS idx_accounts_created ON accounts (created_at);
CREATE INDEX IF NOT EXISTS idx_accounts_lastlogin ON accounts (last_login);
CREATE INDEX IF NOT EXISTS accounts_tags_gin ON accounts USING gin (tags);
CREATE INDEX IF NOT EXISTS accounts_prefs_gin ON accounts USING gin (preferences);

CREATE INDEX IF NOT EXISTS idx_orders_account ON orders (account_id);
CREATE INDEX IF NOT EXISTS idx_orders_created ON orders (created_at);
CREATE INDEX IF NOT EXISTS orders_meta_gin ON orders USING gin (metadata);

CREATE INDEX IF NOT EXISTS idx_orderitems_order ON order_items (order_id, order_created);
CREATE INDEX IF NOT EXISTS idx_orderitems_product ON order_items (product_id);

CREATE INDEX IF NOT EXISTS idx_payments_order ON payments (order_id, order_created);
CREATE INDEX IF NOT EXISTS idx_payments_paidat ON payments (paid_at);

CREATE INDEX IF NOT EXISTS idx_reviews_product ON reviews (product_id);
CREATE INDEX IF NOT EXISTS idx_sessions_account ON sessions (account_id);

CREATE INDEX IF NOT EXISTS idx_events_account ON events (account_id);
CREATE INDEX IF NOT EXISTS idx_events_created ON events (created_at);
CREATE INDEX IF NOT EXISTS events_payload_gin ON events USING gin (payload);
"""

ANALYZE_SQL = "VACUUM (ANALYZE);"

# ----------------- Вспомогательные функции генерации -----------------
def estimate_core_bytes():
    users = BASE_USERS * 300
    products = BASE_PRODUCTS * 300
    categories = BASE_CATEGORIES * 200
    warehouses = BASE_WAREHOUSES * 200
    orders = BASE_ORDERS * 220
    order_items = int(BASE_ORDERS * AVG_ITEMS_PER_ORDER) * 120
    payments = BASE_ORDERS * 80
    reviews = BASE_REVIEWS * 300
    sessions = BASE_SESSIONS * 150
    inventory = BASE_PRODUCTS * 2 * 80
    return users + products + categories + warehouses + orders + order_items + payments + reviews + sessions + inventory

def plan_event_rows(target_gb):
    target_bytes = int(target_gb * (1024 ** 3))
    core = estimate_core_bytes()
    remain = max(0, target_bytes - core)
    if remain == 0:
        return 2_000_000
    rows = int(remain / AVG_EVENT_BYTES)
    rows = max(2_000_000, (rows // 100_000) * 100_000)
    return rows

def write_rows_csv(filename, rows, header=None, gzip_enabled=False):
    if gzip_enabled:
        with gzip.open(filename + ".gz", "wt", newline="") as f:
            w = csv.writer(f)
            if header:
                w.writerow(header)
            w.writerows(rows)
        return filename + ".gz"
    else:
        with open(filename, "w", newline="") as f:
            wtr = csv.writer(f)
            if header:
                wtr.writerow(header)
            wtr.writerows(rows)
        return filename

def gen_id_ranges(total):
    ranges = []
    for i in range(0, total, CHUNK):
        start = i
        end = min(i + CHUNK, total)
        ranges.append((start, end))
    return ranges

# --------------- генераторы конкретных таблиц (чистые функции) ---------------
def gen_categories(total):
    rows = []
    id_seq = 1
    # простая иерархия
    for i in range(total):
        parent = id_seq - random.randint(0, min(10, id_seq-1)) if id_seq > 1 and random.random() < 0.3 else None
        rows.append([id_seq, f"category_{id_seq}", parent, f"path_{id_seq}", fake.date_time_between(START_DATE, TODAY)])
        id_seq += 1
    return rows

def gen_warehouses(total):
    rows = []
    for i in range(total):
        code = f"W{1000+i}"
        rows.append([i+1, code, f"Warehouse {code}", random.choice(COUNTRIES), random.choice(WAREHOUSE_TZS),
                     round(random.uniform(-70,70),5), round(random.uniform(-170,170),5), fake.date_time_between(START_DATE, TODAY)])
    return rows

def gen_accounts(start, end):
    out = []
    for i in range(start, end):
        created = fake.date_time_between(START_DATE, TODAY)
        last_login = created + timedelta(days=random.randint(0, 600)) if random.random() < 0.85 else None
        prefs = {"email_promos": random.random() < 0.6, "lang": random.choice(["en","de","pl","fr","es","it"]),
                 "currency": random.choice(CURRENCIES), "theme": random.choice(["light","dark"])}
        tags = list({random.choice(USER_TAGS) for _ in range(np.random.poisson(1.2))})
        out.append([i+1, str(uuid.uuid4()), fake.name(), f"user{i}@example.com",
                    fake.phone_number() if random.random() < 0.9 else None,
                    random.random() > 0.02, created, last_login, prefs, tags if tags else None])
    return out

def gen_addresses(start, end):
    out = []
    id_seq = start*2 + 1
    for i in range(start, end):
        account_id = i+1
        for t in ("billing", "shipping"):
            if random.random() < 0.85 or t == "billing":
                dt = fake.date_time_between(START_DATE, TODAY)
                out.append([id_seq, account_id, t, random.choice(COUNTRIES), fake.city(), fake.postcode(), fake.street_address(), dt, f"({round(random.uniform(-70,70),6)},{round(random.uniform(-170,170),6)})"])
                id_seq += 1
    return out

def gen_products(start, end, category_ids):
    out = []
    for i in range(start, end):
        attrs = {"color": random.choice(["red","green","blue","black","white","silver","gold"]), "size": random.choice(["XS","S","M","L","XL"]), "material": random.choice(["cotton","plastic","metal","leather","glass","wood"]), "warranty_months": random.choice([6,12,24,36])}
        tags = list({random.choice(PRODUCT_TAGS) for _ in range(np.random.poisson(1.1))})
        price = round(float(np.random.lognormal(mean=4.8, sigma=0.6)), 2)
        out.append([i+1, f"SKU-{i+1:08d}", fake.word() + " " + fake.word(), random.choice(category_ids), price, random.choice(CURRENCIES), attrs, tags if tags else None, int(abs(np.random.normal(500,300))), fake.date_time_between(START_DATE, TODAY)])
    return out

def gen_inventory(product_ids, warehouse_ids):
    now = fake.date_time_between(START_DATE, TODAY)
    rows = []
    for pid in product_ids:
        k = random.randint(1, min(3, len(warehouse_ids)))
        for wid in random.sample(warehouse_ids, k=k):
            qty = max(0, int(abs(np.random.normal(50,80))))
            res = min(qty, int(abs(np.random.normal(5,10))))
            rows.append([pid, wid, qty, res, now])
    return rows

def gen_orders_chunk(start, end, account_ids):
    out = []
    for i in range(start, end):
        created = fake.date_time_between(START_DATE, TODAY)
        status = random.choices(["new","paid","processing","shipped","delivered","cancelled","returned"], weights=[5,20,10,15,30,8,2], k=1)[0]
        total = round(float(np.random.lognormal(mean=4.5, sigma=0.7)), 2)
        meta = {"channel": random.choice(["web","mobile","support"]), "campaign": fake.word() if random.random() < 0.2 else None}
        out.append([i+1, random.choice(account_ids), status, total, random.choice(CURRENCIES), created, created + timedelta(days=random.randint(0,10)), meta])
    return out

def gen_order_items_from_orders(order_rows, product_ids, start_idx):
    out = []
    idx = start_idx
    for order_row in order_rows:
        order_id = order_row[0]
        created_at = order_row[5]
        n_items = max(1, int(np.random.poisson(AVG_ITEMS_PER_ORDER)))
        for _ in range(n_items):
            pid = random.choice(product_ids) if random.random() < 0.2 else product_ids[int(np.random.zipf(1.3)) % len(product_ids)]
            qty = max(1, int(abs(np.random.normal(2,1.5))))
            unit_price = round(float(np.random.lognormal(mean=4.6, sigma=0.6)), 2)
            discount = round(unit_price * qty * (random.choice([0,0,0.05,0.1,0.15]) if random.random() < 0.3 else 0), 2)
            out.append([idx+1, order_id, created_at, pid, qty, unit_price, discount])
            idx += 1
    return out

def gen_payments_from_orders(order_rows, start_idx):
    out = []
    idx = start_idx
    for order_row in order_rows:
        order_id = order_row[0]
        created_at = order_row[5]
        status = random.choices(["pending","authorized","captured","refunded","failed"], weights=[5,15,70,5,5])[0]
        method = random.choices(["card","cash","paypal","crypto","bank_transfer","apple_pay","google_pay"], weights=[60,5,15,3,10,4,3])[0]
        paid_at = created_at + timedelta(minutes=random.randint(1,60)) if status in ("authorized","captured","refunded") else None
        details = {"auth_code": fake.bothify(text="????-########") if status != "failed" else None}
        amount = round(float(np.random.lognormal(mean=4.5, sigma=0.7)), 2)
        out.append([idx+1, order_id, created_at, amount, method, status, details, paid_at])
        idx += 1
    return out

def gen_reviews(start, end, product_ids, account_ids):
    out = []
    for i in range(start, end):
        out.append([i+1, random.choice(product_ids), random.choice(account_ids), int(np.clip(int(np.random.normal(4,1)),1,5)), fake.sentence(nb_words=6), fake.paragraph(nb_sentences=3), max(0, int(abs(np.random.normal(3,5)))), fake.date_time_between(START_DATE, TODAY)])
    return out

def gen_sessions(start, end, account_ids):
    out = []
    for i in range(start, end):
        aid = random.choice(account_ids) if random.random() < 0.7 else None
        start_at = fake.date_time_between(START_DATE, TODAY)
        dur = timedelta(minutes=max(1, int(abs(np.random.normal(12,10)))))
        out.append([str(uuid.uuid4()), aid, fake.ipv4_public(), fake.user_agent(), start_at, start_at + dur if random.random() < 0.95 else None])
    return out

def gen_events(start, end, account_ids):
    out = []
    for i in range(start, end):
        t = random.choice(EVENT_TYPES)
        created = fake.date_time_between(START_DATE, TODAY)
        payload = {"request_id": str(uuid.uuid4()), "ua": fake.user_agent(), "path": "/" + "/".join(fake.words(nb=random.randint(1,4))), "latency_ms": int(abs(np.random.normal(120,90))), "ab": random.choice(["A","B","C"]), "extra": fake.paragraph(nb_sentences=5)}
        out.append([i+1, random.choice(account_ids) if random.random() < 0.8 else None, t, payload, created])
    return out

# ----------------- Функции загрузки в БД -----------------
def run_sql(sql):
    conn = psycopg2.connect(DB_CONN)
    conn.autocommit = True
    cur = conn.cursor()
    try:
        cur.execute(sql)
    finally:
        cur.close()
        conn.close()

def copy_from_files(table, columns, files):
    conn = psycopg2.connect(DB_CONN)
    cur = conn.cursor()
    try:
        for fpath in tqdm(files, desc=f"COPY {table}"):
            if fpath.endswith(".gz"):
                # COPY FROM PROGRAM 'gzip -dc <file>' требует разрешений у сервера.
                cur.execute(f"COPY {table} ({','.join(columns)}) FROM PROGRAM 'gzip -dc {fpath}' WITH (FORMAT csv, HEADER true)")
            else:
                with open(fpath, "r") as f:
                    cur.copy_expert(f"COPY {table} ({','.join(columns)}) FROM STDIN WITH (FORMAT csv, HEADER true)", f)
            conn.commit()
    finally:
        cur.close()
        conn.close()

# ----------------- Параллельная генерация CSV -----------------
def parallel_write(generate_func, total, prefix, extra_args=()):
    files = []
    ranges = gen_id_ranges(total)
    # генерируем последовательно в основном процессе, чтобы избежать проблем с shared state
    # (в некоторых системах multiprocessing + Faker/np может вести себя нестабильно при spawn)
    for idx, (start, end) in enumerate(tqdm(ranges, desc=f"Generate {prefix}")):
        fn = os.path.join(CSV_DIR, f"{prefix}_{idx:05d}.csv")
        rows = generate_func(start, end, *extra_args) if extra_args else generate_func(start, end)
        written = write_rows_csv(fn, rows, header=None, gzip_enabled=USE_GZIP)
        files.append(written)
    return files

def write_static_rows(rows, prefix):
    fn = os.path.join(CSV_DIR, f"{prefix}.csv")
    written = write_rows_csv(fn, rows, header=None, gzip_enabled=USE_GZIP)
    return [written]

# ----------------- MAIN -----------------
def main():
    print("Building schema SQL and creating tables (in default schema)...")
    run_sql(build_schema_sql())

    # Сколько событий нужно, чтобы добить до TARGET_DB_SIZE_GB
    event_rows = plan_event_rows(TARGET_DB_SIZE_GB)
    print(f"Planned event rows: {event_rows:,}")

    # Справочники
    print("Generating categories and warehouses...")
    categories_rows = gen_categories(BASE_CATEGORIES)
    categories_files = write_static_rows(categories_rows, "categories")
    warehouses_rows = gen_warehouses(BASE_WAREHOUSES)
    warehouses_files = write_static_rows(warehouses_rows, "warehouses")

    # Accounts (огромная таблица)
    print("Generating accounts CSVs...")
    accounts_files = parallel_write(gen_accounts, BASE_USERS, "accounts")

    # Addresses
    print("Generating addresses CSVs...")
    addresses_files = parallel_write(gen_addresses, BASE_USERS, "addresses")

    # Products
    print("Generating products CSVs...")
    category_ids = [r[0] for r in categories_rows]
    # products по чанкам
    prod_files = []
    for idx, (s,e) in enumerate(tqdm(gen_id_ranges(BASE_PRODUCTS), desc="Generate products")):
        fn = os.path.join(CSV_DIR, f"products_{idx:05d}.csv")
        rows = gen_products(s, e, category_ids)
        prod_files.append(write_rows_csv(fn, rows, gzip_enabled=USE_GZIP))
    products_files = prod_files
    product_ids_all = list(range(1, BASE_PRODUCTS+1))

    # Inventory
    print("Generating inventory...")
    inv_files = []
    inv_chunk = 200_000
    for start in tqdm(range(1, BASE_PRODUCTS+1, inv_chunk), desc="Inventory chunks"):
        part_ids = list(range(start, min(start+inv_chunk, BASE_PRODUCTS+1)))
        part = gen_inventory(part_ids, [w[0] for w in warehouses_rows])
        fn = os.path.join(CSV_DIR, f"inventory_{start:08d}.csv")
        inv_files.append(write_rows_csv(fn, part, gzip_enabled=USE_GZIP))

    # Orders + order_items + payments (генерируем по чанкам, чтобы связать)
    print("Generating orders, order_items and payments...")
    orders_files, items_files, payments_files = [], [], []
    account_ids = list(range(1, BASE_USERS+1))
    order_ranges = gen_id_ranges(BASE_ORDERS)
    item_global_idx = 0
    payment_global_idx = 0
    for idx, (s,e) in enumerate(tqdm(order_ranges, desc="Orders chunks")):
        orders_chunk = gen_orders_chunk(s, e, account_ids)
        fn_o = os.path.join(CSV_DIR, f"orders_{idx:05d}.csv")
        orders_files.append(write_rows_csv(fn_o, orders_chunk, gzip_enabled=USE_GZIP))

        items_rows = gen_order_items_from_orders(orders_chunk, product_ids_all, item_global_idx)
        fn_i = os.path.join(CSV_DIR, f"order_items_{idx:05d}.csv")
        items_files.append(write_rows_csv(fn_i, items_rows, gzip_enabled=USE_GZIP))
        item_global_idx += len(items_rows)

        pays_rows = gen_payments_from_orders(orders_chunk, payment_global_idx)
        fn_p = os.path.join(CSV_DIR, f"payments_{idx:05d}.csv")
        payments_files.append(write_rows_csv(fn_p, pays_rows, gzip_enabled=USE_GZIP))
        payment_global_idx += len(pays_rows)

    # Reviews
    print("Generating reviews...")
    reviews_files = parallel_write(lambda s,e: gen_reviews(s,e,product_ids_all,account_ids), BASE_REVIEWS, "reviews")

    # Sessions
    print("Generating sessions...")
    sessions_files = parallel_write(lambda s,e: gen_sessions(s,e,account_ids), BASE_SESSIONS, "sessions")

    # Events (балласт для размера)
    print("Generating events...")
    events_files = parallel_write(lambda s,e: gen_events(s,e,account_ids), event_rows, "events")

    # ----------------- COPY в Postgres -----------------
    print("COPY into Postgres (order: small -> big)...")
    # небольшие
    copy_from_files("categories", ["id","name","parent_id","path","created_at"], categories_files)
    copy_from_files("warehouses", ["id","code","name","country","tz","latitude","longitude","created_at"], warehouses_files)

    # крупные
    copy_from_files("accounts", ["id","uuid","full_name","email","phone","is_active","created_at","last_login","preferences","tags"], accounts_files)
    copy_from_files("addresses", ["id","account_id","type","country","city","postal_code","street","created_at","geo"], addresses_files)
    copy_from_files("products", ["id","sku","name","category_id","price","currency","attributes","tags","weight_grams","created_at"], products_files)
    copy_from_files("inventory", ["product_id","warehouse_id","qty","reserved","updated_at"], inv_files)

    copy_from_files("orders", ["id","account_id","status","total_amount","currency","created_at","updated_at","metadata"], orders_files)
    copy_from_files("order_items", ["id","order_id","order_created","product_id","qty","unit_price","discount"], items_files)
    copy_from_files("payments", ["id","order_id","order_created","amount","method","status","details","paid_at"], payments_files)

    copy_from_files("reviews", ["id","product_id","account_id","rating","title","body","helpful_count","created_at"], reviews_files)
    copy_from_files("sessions", ["id","account_id","ip","user_agent","started_at","ended_at"], sessions_files)
    copy_from_files("events", ["id","account_id","type","payload","created_at"], events_files)

    # ----------------- ИНДЕКСЫ И АНАЛИЗ -----------------
    print("Creating indexes and analyzing (this can take time)...")
    run_sql(INDEXES_SQL)
    run_sql(ANALYZE_SQL)

    print("✅ Done. Synthetic DB generated and loaded.")

if __name__ == "__main__":
    main()
