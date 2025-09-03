#!/usr/bin/env python3
"""
gen_mysql_heavy_full.py
Создаёт 10 «тяжёлых» таблиц и заполняет их данными батчами.
Цель размера: TARGET_GB (по умолчанию 8.0 GB).
Работает через pymysql и INSERT (executemany).
"""

import os
import sys
import time
import math
import random
import string
import base64
from datetime import datetime, timedelta
import json

try:
    import pymysql
except Exception:
    print("Please install pymysql: pip install pymysql")
    raise

# -----------------------
# Настройки подключения (заполните ваши данные или используйте строку из вопроса)
# -----------------------
DB = {
    "host": "localhost",
    "port": 3306,
    "user": "gen_user",
    "password": "Passwd123",
    "database": "default_db",
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.Cursor,
}

# -----------------------
# Параметры генерации
# -----------------------
TARGET_GB = 10.0           # целевой размер базы в ГБ (измените: 5..30)
MODE = "insert"           # только 'insert' поддержан по умолчанию (без LOAD DATA)
BATCH_SIZE = 1000         # строки за INSERT executemany
COMMIT_EVERY = 20000      # количество строк до вызова conn.commit()
VERBOSE = True

# Эмпирические средние размеры строки в байтах (используются для расчёта количества строк)
# Значения приблизительные — можно подправить под требования
EST_ROW_BYTES = {
    "users": 1200,
    "products": 2000,
    "orders": 300,
    "logs": 2500,
    "audit_trail": 2200,
    "files": 150000,      # base64-строка ~100-120 KB на запись (регулируется)
    "metrics": 400,
    "messages": 1800,
    "sessions": 800,
    "payments": 700,
}

# Как делить объём по таблицам (сумма = 1.0)
SPLIT = {
    "users": 0.07,
    "products": 0.10,
    "orders": 0.20,
    "logs": 0.15,
    "audit_trail": 0.12,
    "files": 0.15,
    "metrics": 0.07,
    "messages": 0.06,
    "sessions": 0.04,
    "payments": 0.04,
}

# -----------------------
# SQL для создания таблиц (широкие таблицы, много индексов)
# -----------------------
TABLE_SQL = {
"users": """
CREATE TABLE IF NOT EXISTS users (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(80),
    email VARCHAR(200),
    full_name VARCHAR(200),
    profile JSON,
    bio TEXT,
    country VARCHAR(50),
    created_at DATETIME,
    last_login DATETIME,
    flags INT,
    INDEX idx_username (username(32)),
    INDEX idx_email (email(128)),
    INDEX idx_country (country)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""",

"products": """
CREATE TABLE IF NOT EXISTS products (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    sku VARCHAR(100),
    name VARCHAR(300),
    category VARCHAR(120),
    attributes JSON,
    description TEXT,
    price DECIMAL(10,4),
    stock INT,
    vendor VARCHAR(150),
    created_at DATETIME,
    INDEX idx_sku (sku(64)),
    INDEX idx_category (category),
    INDEX idx_vendor (vendor(64))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""",

"orders": """
CREATE TABLE IF NOT EXISTS orders (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    user_id BIGINT UNSIGNED,
    product_id BIGINT UNSIGNED,
    qty SMALLINT,
    unit_price DECIMAL(10,4),
    tax DECIMAL(10,4),
    shipping DECIMAL(10,4),
    total DECIMAL(12,4),
    status VARCHAR(50),
    created_at DATETIME,
    INDEX idx_user_id (user_id),
    INDEX idx_product_id (product_id),
    INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""",

"logs": """
CREATE TABLE IF NOT EXISTS logs (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    level VARCHAR(16),
    service VARCHAR(120),
    host VARCHAR(120),
    pid INT,
    thread VARCHAR(80),
    message TEXT,
    context JSON,
    created_at DATETIME,
    INDEX idx_level (level),
    INDEX idx_service (service(60)),
    INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""",

"audit_trail": """
CREATE TABLE IF NOT EXISTS audit_trail (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    user_id BIGINT UNSIGNED,
    object_type VARCHAR(80),
    object_id VARCHAR(80),
    action VARCHAR(80),
    payload JSON,
    old_value TEXT,
    new_value TEXT,
    ip VARCHAR(45),
    created_at DATETIME,
    INDEX idx_user_id (user_id),
    INDEX idx_object_type (object_type(32)),
    INDEX idx_action (action(32)),
    INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""",

"files": """
CREATE TABLE IF NOT EXISTS files (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    owner_id BIGINT UNSIGNED,
    filename VARCHAR(300),
    mime VARCHAR(100),
    size_bytes BIGINT UNSIGNED,
    content_base64 MEDIUMTEXT,
    metadata JSON,
    created_at DATETIME,
    INDEX idx_owner_id (owner_id),
    INDEX idx_filename (filename(120))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=COMPRESSED;
""",

"metrics": """
CREATE TABLE IF NOT EXISTS metrics (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    metric_name VARCHAR(200),
    ts DATETIME,
    value DOUBLE,
    tags JSON,
    sample_rate INT,
    created_at DATETIME,
    INDEX idx_metric_name (metric_name(100)),
    INDEX idx_ts (ts)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""",

"messages": """
CREATE TABLE IF NOT EXISTS messages (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    from_user BIGINT UNSIGNED,
    to_user BIGINT UNSIGNED,
    subject VARCHAR(300),
    body TEXT,
    attachments JSON,
    is_read TINYINT,
    created_at DATETIME,
    INDEX idx_from_user (from_user),
    INDEX idx_to_user (to_user),
    FULLTEXT KEY ft_subject_body (subject, body)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""",

"sessions": """
CREATE TABLE IF NOT EXISTS sessions (
    id CHAR(36) NOT NULL PRIMARY KEY,
    user_id BIGINT UNSIGNED,
    token VARCHAR(255),
    data JSON,
    created_at DATETIME,
    expires_at DATETIME,
    INDEX idx_user_id (user_id),
    INDEX idx_expires_at (expires_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
""",

"payments": """
CREATE TABLE IF NOT EXISTS payments (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    order_id BIGINT UNSIGNED,
    user_id BIGINT UNSIGNED,
    amount DECIMAL(12,4),
    currency CHAR(3),
    method VARCHAR(80),
    status VARCHAR(80),
    details TEXT,
    created_at DATETIME,
    INDEX idx_order_id (order_id),
    INDEX idx_user_id (user_id),
    INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""
}

# -----------------------
# Генераторы данных (достаточно разнообразно и тяжелые поля)
# -----------------------
COUNTRIES = ["US","NL","DE","RU","CN","IN","GB","FR","ES","IT","JP","BR","CA","AU"]
SERVICES = ["auth","payments","worker","api","cron","scheduler","ui","db"]
EVENT_TYPES = ["click","view","purchase","login","logout","update","create","delete"]

def rnd_string(min_len=10, max_len=60):
    l = random.randint(min_len, max_len)
    return ''.join(random.choices(string.ascii_letters + string.digits + "   ", k=l)).strip()

def rnd_text(min_len=100, max_len=2000):
    size = random.randint(min_len, max_len)
    words = []
    while sum(len(w)+1 for w in words) < size:
        wlen = random.randint(3,12)
        words.append(''.join(random.choices(string.ascii_lowercase, k=wlen)))
    return ' '.join(words)[:size]

def rnd_json_kv(n=5, depth=1):
    obj = {}
    for i in range(n):
        k = "k"+str(i)
        if depth > 0 and random.random() < 0.3:
            obj[k] = rnd_json_kv(n=3, depth=depth-1)
        else:
            obj[k] = rnd_string(3,20)
    return obj

def gen_user(i):
    username = f"user{i}_{rnd_string(4,8)}"
    email = f"{username}@example.com"
    full_name = rnd_string(10,40)
    profile = rnd_json_kv(6, depth=2)
    bio = rnd_text(200, 1200)
    country = random.choice(COUNTRIES)
    created = datetime.now() - timedelta(days=random.randint(0, 3650))
    last_login = created + timedelta(days=random.randint(0, 3000))
    flags = random.randint(0, 255)
    return (
        username,
        email,
        full_name,
        json.dumps(profile, ensure_ascii=False),  # ✅ правильный JSON
        bio,
        country,
        created.strftime("%Y-%m-%d %H:%M:%S"),
        last_login.strftime("%Y-%m-%d %H:%M:%S"),
        flags
    )

def gen_product(i):
    sku = f"SKU-{i:012d}"
    name = " ".join([rnd_string(4,12) for _ in range(6)])[:300]
    category = random.choice(["electronics","books","clothing","home","garden","sports","auto","software"])
    attributes = rnd_json_kv(10, depth=2)
    description = rnd_text(400, 2000)
    price = round(random.uniform(0.5, 10000.0), 4)
    stock = random.randint(0, 10000)
    vendor = rnd_string(6, 40)
    created = datetime.now() - timedelta(days=random.randint(0, 3650))
    return (sku, name, category, json.dumps(attributes, ensure_ascii=False), description, price, stock, vendor, created.strftime("%Y-%m-%d %H:%M:%S"))

def gen_order(i, max_user_id, max_product_id):
    user_id = random.randint(1, max(1, max_user_id))
    product_id = random.randint(1, max(1, max_product_id))
    qty = random.randint(1, 20)
    unit_price = round(random.uniform(0.5, 5000.0), 4)
    tax = round(unit_price * 0.1, 4)
    shipping = round(random.uniform(0, 50.0), 4)
    total = round(unit_price * qty + tax + shipping, 4)
    status = random.choice(["new","processing","shipped","delivered","cancelled","failed"])
    created = datetime.now() - timedelta(days=random.randint(0, 3650))
    return (user_id, product_id, qty, unit_price, tax, shipping, total, status, created.strftime("%Y-%m-%d %H:%M:%S"))

def gen_log(i):
    level = random.choice(["DEBUG","INFO","WARN","ERROR","CRITICAL"])
    service = random.choice(SERVICES)
    host = f"host{random.randint(1,200)}"
    pid = random.randint(100, 99999)
    thread = rnd_string(6, 20)
    message = rnd_text(300, 2500)
    context = rnd_json_kv(6, depth=2)
    created = datetime.now() - timedelta(seconds=random.randint(0, 60*60*24*365))
    return (level, service, host, pid, thread, message, json.dumps(context, ensure_ascii=False), created.strftime("%Y-%m-%d %H:%M:%S"))

def gen_audit(i):
    user_id = random.randint(1, 1000000)
    object_type = random.choice(["order","product","user","session","payment","file"])
    object_id = str(random.randint(1, 1000000000))
    action = random.choice(["create","update","delete","access","permission_change"])
    payload = rnd_json_kv(10, depth=2)
    old = rnd_text(50, 800)
    new = rnd_text(50, 800)
    ip = f"{random.randint(1,255)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(0,255)}"
    created = datetime.now() - timedelta(days=random.randint(0, 3650))
    return (user_id, object_type, object_id, action, json.dumps(payload, ensure_ascii=False), old, new, ip, created.strftime("%Y-%m-%d %H:%M:%S"))

def gen_file(i):
    owner_id = random.randint(1, 1000000)
    filename = f"file_{i}_{rnd_string(6,20)}.dat"
    mime = random.choice(["application/octet-stream","image/png","application/pdf","text/plain","application/zip"])
    # size around 100KB..300KB (adjust if needed)
    size = random.randint(100*1024, 300*1024)
    # random bytes then base64 to simulate content stored as base64 MEDIUMTEXT
    b = os.urandom(size)
    b64 = base64.b64encode(b).decode('ascii')
    metadata = rnd_json_kv(6, depth=1)
    created = datetime.now() - timedelta(days=random.randint(0, 3650))
    return (owner_id, filename, mime, size, b64, json.dumps(metadata, ensure_ascii=False), created.strftime("%Y-%m-%d %H:%M:%S"))

def gen_metric(i):
    metric_name = random.choice(["cpu.usage","mem.usage","disk.io","http.requests","db.connections","latency"])
    ts = datetime.now() - timedelta(seconds=random.randint(0, 60*60*24*365))
    value = random.random() * 1000.0
    tags = rnd_json_kv(5, depth=1)
    sample_rate = random.choice([1,5,10,60])
    created = ts
    return (metric_name, ts.strftime("%Y-%m-%d %H:%M:%S"), value, json.dumps(tags, ensure_ascii=False), sample_rate, created.strftime("%Y-%m-%d %H:%M:%S"))

def gen_message(i, max_user_id):
    from_user = random.randint(1, max(1, max_user_id))
    to_user = random.randint(1, max(1, max_user_id))
    subject = rnd_string(20, 200)[:300]
    body = rnd_text(200, 3000)
    attachments = rnd_json_kv(3, depth=1)
    is_read = random.choice([0,1])
    created = datetime.now() - timedelta(days=random.randint(0, 3650))
    return (from_user, to_user, subject, body, json.dumps(attachments, ensure_ascii=False), is_read, created.strftime("%Y-%m-%d %H:%M:%S"))

def gen_session(i, max_user_id):
    sid = ''.join(random.choices('0123456789abcdef', k=36))
    user_id = random.randint(1, max(1, max_user_id))
    token = ''.join(random.choices(string.ascii_letters + string.digits, k=120))
    data = rnd_json_kv(8, depth=2)
    created = datetime.now() - timedelta(days=random.randint(0, 3650))
    expires = created + timedelta(days=random.randint(1, 365))
    return (sid, user_id, token, json.dumps(data, ensure_ascii=False), created.strftime("%Y-%m-%d %H:%M:%S"), expires.strftime("%Y-%m-%d %H:%M:%S"))

def gen_payment(i, max_user_id):
    order_id = random.randint(1, 20000000)
    user_id = random.randint(1, max(1, max_user_id))
    amount = round(random.uniform(0.1, 20000.0), 4)
    currency = random.choice(["USD","EUR","RUB","CNY","GBP"])
    method = random.choice(["card","paypal","bank","crypto","apple_pay"])
    status = random.choice(["ok","failed","pending","refunded"])
    details = rnd_text(50, 400)
    created = datetime.now() - timedelta(days=random.randint(0, 3650))
    return (order_id, user_id, amount, currency, method, status, details, created.strftime("%Y-%m-%d %H:%M:%S"))

# -----------------------
# Вспомогательные: расчет количества строк для каждой таблицы
# -----------------------
def estimate_rows_per_table(target_gb):
    total_bytes = target_gb * (1024**3)
    rows = {}
    for t, frac in SPLIT.items():
        bytes_for = total_bytes * frac
        est = EST_ROW_BYTES.get(t, 500)
        rows[t] = max(1, int(bytes_for / est))
    return rows

# -----------------------
# SQL и вызов генераторов (map)
# -----------------------
INSERT_SQL = {
    "users": ("INSERT INTO users (username,email,full_name,profile,bio,country,created_at,last_login,flags) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
              gen_user),
    "products": ("INSERT INTO products (sku,name,category,attributes,description,price,stock,vendor,created_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                 gen_product),
    "orders": ("INSERT INTO orders (user_id,product_id,qty,unit_price,tax,shipping,total,status,created_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
               gen_order),
    "logs": ("INSERT INTO logs (level,service,host,pid,thread,message,context,created_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
             gen_log),
    "audit_trail": ("INSERT INTO audit_trail (user_id,object_type,object_id,action,payload,old_value,new_value,ip,created_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    gen_audit),
    "files": ("INSERT INTO files (owner_id,filename,mime,size_bytes,content_base64,metadata,created_at) VALUES (%s,%s,%s,%s,%s,%s,%s)",
              gen_file),
    "metrics": ("INSERT INTO metrics (metric_name,ts,value,tags,sample_rate,created_at) VALUES (%s,%s,%s,%s,%s,%s)",
                gen_metric),
    "messages": ("INSERT INTO messages (from_user,to_user,subject,body,attachments,is_read,created_at) VALUES (%s,%s,%s,%s,%s,%s,%s)",
                 gen_message),
    "sessions": ("INSERT INTO sessions (id,user_id,token,data,created_at,expires_at) VALUES (%s,%s,%s,%s,%s,%s)",
                 gen_session),
    "payments": ("INSERT INTO payments (order_id,user_id,amount,currency,method,status,details,created_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                 gen_payment),
}

# -----------------------
# Основные функции вставки
# -----------------------
def connect():
    return pymysql.connect(host=DB["host"], port=DB["port"],
                           user=DB["user"], password=DB["password"],
                           database=DB["database"], charset=DB["charset"],
                           cursorclass=DB["cursorclass"], autocommit=False)

def create_tables(conn):
    cur = conn.cursor()
    for name, sql in TABLE_SQL.items():
        if VERBOSE:
            print(f"[DDL] creating table {name} ...")
        cur.execute(sql)
    conn.commit()

def batched_insert(conn, table, count, gen_fn, batch_size=BATCH_SIZE, commit_every=COMMIT_EVERY, start_index=0, max_user_id_hint=1000000, max_product_id_hint=1000000):
    cur = conn.cursor()
    sql, _ = INSERT_SQL[table]
    inserted = 0
    to_commit = 0
    t0 = time.time()
    batch = []
    while inserted < count:
        idx = start_index + inserted + 1
        # generate row based on table specifics
        if table == "orders":
            row = gen_fn(idx, max_user_id_hint, max_product_id_hint)
        elif table == "messages":
            row = gen_fn(idx, max_user_id_hint)
        elif table == "sessions":
            row = gen_fn(idx, max_user_id_hint)
        elif table == "payments":
            row = gen_fn(idx, max_user_id_hint)
        else:
            # generic single-arg generator
            row = gen_fn(idx)
        batch.append(row)
        if len(batch) >= batch_size:
            cur.executemany(sql, batch)
            inserted += len(batch)
            to_commit += len(batch)
            batch = []
            if to_commit >= commit_every:
                conn.commit()
                to_commit = 0
            if VERBOSE and inserted % (batch_size*5) == 0:
                elapsed = time.time() - t0
                print(f"[{table}] inserted {inserted}/{count} rows, elapsed {elapsed:.1f}s")
    # final flush
    if batch:
        cur.executemany(sql, batch)
        inserted += len(batch)
    conn.commit()
    if VERBOSE:
        print(f"[{table}] done. inserted {inserted} rows in {time.time()-t0:.1f}s")
    return inserted

# -----------------------
# Основной поток
# -----------------------
def main():
    print(f"Target ~= {TARGET_GB} GB; mode={MODE}; batch_size={BATCH_SIZE}")
    rows_plan = estimate_rows_per_table(TARGET_GB)
    print("Estimated rows per table (approx):")
    for k,v in rows_plan.items():
        print(f"  {k:12s}: {v:,}")

    conn = connect()
    try:
        create_tables(conn)
        # We will insert tables in an order so that orders/messages refer to users/products counts
        # Keep track of max id hints (we assume autoincrement starts at 1)
        max_ids = {}

        # users
        t = "users"
        cnt = rows_plan[t]
        print(f"\nInserting {cnt:,} rows into {t} ...")
        inserted = batched_insert(conn, t, cnt, gen_user)
        max_ids["users"] = inserted

        # products
        t = "products"
        cnt = rows_plan[t]
        print(f"\nInserting {cnt:,} rows into {t} ...")
        inserted = batched_insert(conn, t, cnt, gen_product)
        max_ids["products"] = inserted

        # orders (referencing users/products)
        t = "orders"
        cnt = rows_plan[t]
        print(f"\nInserting {cnt:,} rows into {t} ...")
        inserted = batched_insert(conn, t, cnt, gen_order, max_user_id_hint=max_ids["users"], max_product_id_hint=max_ids["products"])
        max_ids["orders"] = inserted

        # logs
        t = "logs"
        cnt = rows_plan[t]
        print(f"\nInserting {cnt:,} rows into {t} ...")
        inserted = batched_insert(conn, t, cnt, gen_log)
        max_ids["logs"] = inserted

        # audit_trail
        t = "audit_trail"
        cnt = rows_plan[t]
        print(f"\nInserting {cnt:,} rows into {t} ...")
        inserted = batched_insert(conn, t, cnt, gen_audit)
        max_ids["audit_trail"] = inserted

        # files (heavy - base64 content)
        t = "files"
        cnt = rows_plan[t]
        # reduce batch for large rows to avoid memory spikes
        file_batch_size = max(1, min(BATCH_SIZE, 200))
        print(f"\nInserting {cnt:,} rows into {t} (large blobs) ...")
        inserted = batched_insert(conn, t, cnt, gen_file, batch_size=file_batch_size)
        max_ids["files"] = inserted

        # metrics
        t = "metrics"
        cnt = rows_plan[t]
        print(f"\nInserting {cnt:,} rows into {t} ...")
        inserted = batched_insert(conn, t, cnt, gen_metric, batch_size=2000)
        max_ids["metrics"] = inserted

        # messages
        t = "messages"
        cnt = rows_plan[t]
        print(f"\nInserting {cnt:,} rows into {t} ...")
        inserted = batched_insert(conn, t, cnt, gen_message, max_user_id_hint=max_ids["users"])
        max_ids["messages"] = inserted

        # sessions
        t = "sessions"
        cnt = rows_plan[t]
        print(f"\nInserting {cnt:,} rows into {t} ...")
        inserted = batched_insert(conn, t, cnt, gen_session, max_user_id_hint=max_ids["users"])
        max_ids["sessions"] = inserted

        # payments
        t = "payments"
        cnt = rows_plan[t]
        print(f"\nInserting {cnt:,} rows into {t} ...")
        inserted = batched_insert(conn, t, cnt, gen_payment, max_user_id_hint=max_ids["users"])
        max_ids["payments"] = inserted

        print("\nGeneration finished. Verify disk usage and consider running OPTIMIZE TABLE if needed.")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
