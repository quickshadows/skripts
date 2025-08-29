#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Генератор очень большой и «тяжёлой» БД PostgreSQL, приближенной к продакшену.
— 20+ таблиц со связями и индексами
— Потоковая генерация строк без удержания всего в памяти
— Загрузка через COPY ... FROM STDIN (быстро)
— Параметры объёмов через argparse

Требования:
    pip install psycopg2-binary faker

Примеры запуска:
    python3 generate_big_pg_data.py \
      --dsn "host=127.0.0.1 port=5432 dbname=default_db user=gen_user password=Passwd123" \
      --users 1000000 --products 200000 --orders 20000000 --logs 100000000 --messages 30000000

Замечания по производительности:
  * Выполняйте скрипт с быстрым диском (NVMe), рядом с сервером БД.
  * Запускайте на пустой схеме: скрипт может TRUNCATE + RESTART IDENTITY.
  * Индексы создаются ПОСЛЕ загрузки — это быстрее.
  * Можно параллелить несколько скриптов по «самостоятельным» таблицам (логи/метрики/нотификации), если есть свободные CPU/IO.
"""

from __future__ import annotations
import argparse
import io
import json
import math
import os
import random
import string
from datetime import datetime, timedelta
from typing import Iterable, Iterator, List, Tuple

import psycopg2
from psycopg2.extensions import connection as PGConnection
from faker import Faker

# -----------------------------
# Утилиты
# -----------------------------

class IterableIO(io.TextIOBase):
    """Преобразует итератор строк в file-like объект для COPY FROM STDIN."""
    def __init__(self, iterator: Iterator[str]):
        self.iterator = iterator
        self._buffer = ""

    def readable(self):
        return True

    def read(self, n: int = -1) -> str:
        try:
            chunk = next(self.iterator)
            return chunk
        except StopIteration:
            return ""

    def readline(self, limit: int = -1) -> str:
        try:
            return next(self.iterator)
        except StopIteration:
            return ""

# Безопасные CSV-поля (минимум экранирования). COPY CSV сам экранирует кавычки.

def csv_escape(val: str) -> str:
    return val.replace("\n", " ").replace("\r", " ")

# -----------------------------
# Генераторы данных
# -----------------------------

fake = Faker()

ACTIONS = [
    "login", "logout", "view_product", "add_to_cart", "remove_from_cart",
    "checkout", "payment_success", "payment_failed", "update_profile",
    "password_reset", "search", "filter", "wishlist_add", "wishlist_remove"
]

PAY_METHODS = ["card", "paypal", "apple_pay", "google_pay", "bank_transfer"]
PAY_STATUSES = ["new", "authorized", "captured", "failed", "refunded"]
SHIP_STATUSES = ["pending", "packing", "shipped", "in_transit", "delivered", "returned"]
ORDER_STATUSES = ["pending", "paid", "shipped", "cancelled"]
NOTIFY_TYPES = ["system", "marketing", "security", "reminder"]
CURRENCIES = ["USD", "EUR", "GBP"]
ROLE_NAMES = ["admin", "manager", "customer"]

# Временные окна для реалистичных дат
NOW = datetime.utcnow()
START_DATE = NOW - timedelta(days=365 * 3)


def rand_ts() -> datetime:
    delta = NOW - START_DATE
    return START_DATE + timedelta(seconds=random.randint(0, int(delta.total_seconds())))


def hex_bytes(n: int) -> str:
    """HEX для BYTEA через COPY (формат \xDEADBEEF)."""
    return "\\x" + os.urandom(n).hex()


# ---------- Табличные генераторы (строки CSV) ----------
# Внимание: таблицы без явного id используют serial/bigserial, но для ссылок мы
# предполагаем, что таблицы пустые, и id будут 1..N в порядке вставки.


def users_rows(n):
    """Генератор пользователей"""
    for i in range(1, n + 1):
        username = fake.user_name() + str(i)  # добавляем i для уникальности
        email = f"user{i}@example.com"        # чтобы точно не было дублей
        created_at = fake.date_time_this_decade()
        yield (username, email, created_at)



def profiles_rows(n_users: int) -> Iterator[str]:
    # columns: user_id,first_name,last_name,bio,birth_date,country,city
    for uid in range(1, n_users + 1):
        first = csv_escape(fake.first_name())
        last = csv_escape(fake.last_name())
        bio = csv_escape(fake.text(max_nb_chars=500))
        bdate = fake.date_of_birth(minimum_age=18, maximum_age=80).isoformat()
        country = csv_escape(fake.country())
        city = csv_escape(fake.city())
        yield f"{uid},{first},{last},{bio},{bdate},{country},{city}\n"


def roles_rows() -> Iterator[str]:
    for name in ROLE_NAMES:
        yield f"{name}\n"


def user_roles_rows(n_users: int) -> Iterator[str]:
    # каждому пользователю 1-2 роли, с bias в сторону customer
    for uid in range(1, n_users + 1):
        yield f"{uid},3\n"  # customer
        if random.random() < 0.05:
            yield f"{uid},2\n"  # manager
        if random.random() < 0.01:
            yield f"{uid},1\n"  # admin


def categories_rows(n_categories: int) -> Iterator[str]:
    for _ in range(n_categories):
        yield f"{csv_escape(fake.word())}\n"


def products_rows(n_products: int) -> Iterator[str]:
    # name,description,price,stock,created_at
    for _ in range(n_products):
        name = csv_escape(fake.catch_phrase())
        desc = csv_escape(fake.text(max_nb_chars=2000))
        price = f"{random.uniform(3, 5000):.2f}"
        stock = random.randint(0, 10000)
        created = rand_ts().isoformat(sep=" ")
        yield f"{name},{desc},{price},{stock},{created}\n"


def product_categories_rows(n_products: int, n_categories: int) -> Iterator[str]:
    # по 1-3 категории на товар
    for pid in range(1, n_products + 1):
        cats = random.sample(range(1, n_categories + 1), k=random.randint(1, min(3, n_categories)))
        for cid in cats:
            yield f"{pid},{cid}\n"


def orders_rows(n_orders: int, n_users: int) -> Iterator[str]:
    # user_id,status,total,created_at
    for _ in range(n_orders):
        uid = random.randint(1, n_users)
        status = random.choice(ORDER_STATUSES)
        total = f"{random.uniform(5, 20000):.2f}"
        created = rand_ts().isoformat(sep=" ")
        yield f"{uid},{status},{total},{created}\n"


def order_items_rows(n_orders: int, n_products: int, max_items: int = 6) -> Iterator[str]:
    # order_id,product_id,quantity,price
    for oid in range(1, n_orders + 1):
        for _ in range(random.randint(1, max_items)):
            pid = random.randint(1, n_products)
            qty = random.randint(1, 10)
            price = f"{random.uniform(3, 5000):.2f}"
            yield f"{oid},{pid},{qty},{price}\n"


def payments_rows(n_orders: int) -> Iterator[str]:
    # order_id,amount,method,status,paid_at
    for oid in range(1, n_orders + 1):
        if random.random() < 0.95:  # не у всех заказов оплата есть
            amount = f"{random.uniform(5, 20000):.2f}"
            method = random.choice(PAY_METHODS)
            status = random.choice(PAY_STATUSES)
            paid_at = rand_ts().isoformat(sep=" ") if status in ("authorized", "captured", "refunded") else ""
            yield f"{oid},{amount},{method},{status},{paid_at}\n"


def shipments_rows(n_orders: int) -> Iterator[str]:
    # order_id,address,status,shipped_at
    for oid in range(1, n_orders + 1):
        if random.random() < 0.8:
            addr = csv_escape(f"{fake.street_address()}, {fake.city()}, {fake.country()}")
            status = random.choice(SHIP_STATUSES)
            shipped = rand_ts().isoformat(sep=" ") if status not in ("pending", "packing") else ""
            yield f"{oid},{addr},{status},{shipped}\n"


def invoices_rows(n_orders: int) -> Iterator[str]:
    # order_id,pdf,issued_at  (pdf — bytea HEX или пусто)
    for oid in range(1, n_orders + 1):
        if random.random() < 0.7:
            pdf_hex = hex_bytes(random.randint(256, 2048)) if random.random() < 0.2 else ""
            issued = rand_ts().isoformat(sep=" ")
            yield f"{oid},{pdf_hex},{issued}\n"


def transactions_rows(n_users: int, n_transactions: int) -> Iterator[str]:
    # user_id,amount,currency,status,created_at
    for _ in range(n_transactions):
        uid = random.randint(1, n_users)
        amount = f"{random.uniform(-5000, 5000):.2f}"
        curr = random.choice(CURRENCIES)
        status = random.choice(["ok", "hold", "reverted", "failed"]) 
        created = rand_ts().isoformat(sep=" ")
        yield f"{uid},{amount},{curr},{status},{created}\n"


def messages_rows(n_messages: int, n_users: int) -> Iterator[str]:
    # sender_id,receiver_id,body,created_at
    for _ in range(n_messages):
        s = random.randint(1, n_users)
        r = random.randint(1, n_users)
        while r == s:
            r = random.randint(1, n_users)
        body = csv_escape(fake.text(max_nb_chars=1000))
        created = rand_ts().isoformat(sep=" ")
        yield f"{s},{r},{body},{created}\n"


def notifications_rows(n_notifications: int, n_users: int) -> Iterator[str]:
    # user_id,type,content,seen,created_at
    for _ in range(n_notifications):
        uid = random.randint(1, n_users)
        t = random.choice(NOTIFY_TYPES)
        content = csv_escape(fake.text(max_nb_chars=600))
        seen = "true" if random.random() < 0.7 else "false"
        created = rand_ts().isoformat(sep=" ")
        yield f"{uid},{t},{content},{seen},{created}\n"


def chats_rows(n_chats: int) -> Iterator[str]:
    # title,created_at
    for _ in range(n_chats):
        title = csv_escape(fake.bs().title())
        created = rand_ts().isoformat(sep=" ")
        yield f"{title},{created}\n"


def chat_messages_rows(n_chat_messages: int, n_chats: int, n_users: int) -> Iterator[str]:
    # chat_id,user_id,body,created_at
    for _ in range(n_chat_messages):
        chat_id = random.randint(1, n_chats)
        uid = random.randint(1, n_users)
        body = csv_escape(fake.text(max_nb_chars=800))
        created = rand_ts().isoformat(sep=" ")
        yield f"{chat_id},{uid},{body},{created}\n"


def logs_rows(n_logs: int, n_users: int) -> Iterator[str]:
    # user_id,action,details,ip,created_at
    for _ in range(n_logs):
        uid = random.randint(1, n_users)
        action = random.choice(ACTIONS)
        details = csv_escape(fake.text(max_nb_chars=2000))
        ip = fake.ipv4()
        created = rand_ts().isoformat(sep=" ")
        yield f"{uid},{action},{details},{ip},{created}\n"


def metrics_rows(n_metrics: int) -> Iterator[str]:
    # name,value,collected_at
    for _ in range(n_metrics):
        name = csv_escape("metric_" + random.choice(["latency", "qps", "errors", "apdex", "cpu", "io"]))
        value = f"{random.uniform(0, 1000000):.4f}"
        collected = rand_ts().isoformat(sep=" ")
        yield f"{name},{value},{collected}\n"


def audit_rows(n_audit: int, n_users: int) -> Iterator[str]:
    # table_name,row_id,action,changed_at,user_id
    tables = [
        "users", "profiles", "products", "orders", "order_items", "payments",
        "shipments", "invoices", "messages", "notifications", "chats", "chat_messages"
    ]
    acts = ["INSERT", "UPDATE", "DELETE"]
    for _ in range(n_audit):
        t = random.choice(tables)
        row_id = random.randint(1, 10_000_000)
        act = random.choice(acts)
        changed = rand_ts().isoformat(sep=" ")
        uid = random.randint(1, n_users)
        yield f"{t},{row_id},{act},{changed},{uid}\n"

# -----------------------------
# DDL (создание таблиц)
# -----------------------------

DDL = [
    """
    CREATE TABLE IF NOT EXISTS users (
        id BIGSERIAL PRIMARY KEY,
        username VARCHAR(50) UNIQUE,
        email VARCHAR(100) UNIQUE,
        created_at TIMESTAMP NOT NULL DEFAULT now()
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS profiles (
        id BIGSERIAL PRIMARY KEY,
        user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
        first_name VARCHAR(50),
        last_name VARCHAR(50),
        bio TEXT,
        birth_date DATE,
        country VARCHAR(50),
        city VARCHAR(50)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS roles (
        id SERIAL PRIMARY KEY,
        name VARCHAR(50) UNIQUE
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS user_roles (
        user_id BIGINT REFERENCES users(id),
        role_id INT REFERENCES roles(id),
        PRIMARY KEY (user_id, role_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS categories (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS products (
        id BIGSERIAL PRIMARY KEY,
        name VARCHAR(100),
        description TEXT,
        price NUMERIC(10,2),
        stock INT,
        created_at TIMESTAMP NOT NULL DEFAULT now()
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS product_categories (
        product_id BIGINT REFERENCES products(id),
        category_id INT REFERENCES categories(id),
        PRIMARY KEY (product_id, category_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS orders (
        id BIGSERIAL PRIMARY KEY,
        user_id BIGINT REFERENCES users(id),
        status VARCHAR(20),
        total NUMERIC(12,2),
        created_at TIMESTAMP NOT NULL DEFAULT now()
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS order_items (
        id BIGSERIAL PRIMARY KEY,
        order_id BIGINT REFERENCES orders(id) ON DELETE CASCADE,
        product_id BIGINT REFERENCES products(id),
        quantity INT,
        price NUMERIC(10,2)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS payments (
        id BIGSERIAL PRIMARY KEY,
        order_id BIGINT REFERENCES orders(id) ON DELETE CASCADE,
        amount NUMERIC(12,2),
        method VARCHAR(20),
        status VARCHAR(20),
        paid_at TIMESTAMP
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS shipments (
        id BIGSERIAL PRIMARY KEY,
        order_id BIGINT REFERENCES orders(id) ON DELETE CASCADE,
        address TEXT,
        status VARCHAR(20),
        shipped_at TIMESTAMP
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS invoices (
        id BIGSERIAL PRIMARY KEY,
        order_id BIGINT REFERENCES orders(id) ON DELETE CASCADE,
        pdf BYTEA,
        issued_at TIMESTAMP
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS transactions (
        id BIGSERIAL PRIMARY KEY,
        user_id BIGINT REFERENCES users(id),
        amount NUMERIC(12,2),
        currency VARCHAR(3),
        status VARCHAR(20),
        created_at TIMESTAMP
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS messages (
        id BIGSERIAL PRIMARY KEY,
        sender_id BIGINT REFERENCES users(id),
        receiver_id BIGINT REFERENCES users(id),
        body TEXT,
        created_at TIMESTAMP
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS notifications (
        id BIGSERIAL PRIMARY KEY,
        user_id BIGINT REFERENCES users(id),
        type VARCHAR(50),
        content TEXT,
        seen BOOLEAN DEFAULT false,
        created_at TIMESTAMP
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS chats (
        id BIGSERIAL PRIMARY KEY,
        title VARCHAR(100),
        created_at TIMESTAMP
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS chat_messages (
        id BIGSERIAL PRIMARY KEY,
        chat_id BIGINT REFERENCES chats(id) ON DELETE CASCADE,
        user_id BIGINT REFERENCES users(id),
        body TEXT,
        created_at TIMESTAMP
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS logs (
        id BIGSERIAL PRIMARY KEY,
        user_id BIGINT REFERENCES users(id),
        action VARCHAR(100),
        details TEXT,
        ip INET,
        created_at TIMESTAMP
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS metrics (
        id BIGSERIAL PRIMARY KEY,
        name VARCHAR(100),
        value NUMERIC(20,4),
        collected_at TIMESTAMP
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS audit (
        id BIGSERIAL PRIMARY KEY,
        table_name VARCHAR(50),
        row_id BIGINT,
        action VARCHAR(10),
        changed_at TIMESTAMP,
        user_id BIGINT
    );
    """,
]

INDEXES = [
    # создаём ПОСЛЕ загрузки
    "CREATE INDEX IF NOT EXISTS idx_users_created_at ON users(created_at);",
    "CREATE INDEX IF NOT EXISTS idx_profiles_user_id ON profiles(user_id);",
    "CREATE INDEX IF NOT EXISTS idx_products_created_at ON products(created_at);",
    "CREATE INDEX IF NOT EXISTS idx_orders_user_id ON orders(user_id);",
    "CREATE INDEX IF NOT EXISTS idx_orders_created_at ON orders(created_at);",
    "CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON order_items(order_id);",
    "CREATE INDEX IF NOT EXISTS idx_payments_order_id ON payments(order_id);",
    "CREATE INDEX IF NOT EXISTS idx_shipments_order_id ON shipments(order_id);",
    "CREATE INDEX IF NOT EXISTS idx_invoices_order_id ON invoices(order_id);",
    "CREATE INDEX IF NOT EXISTS idx_transactions_user_id ON transactions(user_id);",
    "CREATE INDEX IF NOT EXISTS idx_messages_sender ON messages(sender_id);",
    "CREATE INDEX IF NOT EXISTS idx_messages_receiver ON messages(receiver_id);",
    "CREATE INDEX IF NOT EXISTS idx_notifications_user_id ON notifications(user_id);",
    "CREATE INDEX IF NOT EXISTS idx_chat_messages_chat_id ON chat_messages(chat_id);",
    "CREATE INDEX IF NOT EXISTS idx_logs_user_id ON logs(user_id);",
    "CREATE INDEX IF NOT EXISTS idx_logs_created_at ON logs(created_at);",
    "CREATE INDEX IF NOT EXISTS idx_metrics_collected_at ON metrics(collected_at);",
    "CREATE INDEX IF NOT EXISTS idx_audit_table_row ON audit(table_name, row_id);",
]

# -----------------------------
# COPY-помощники
# -----------------------------

def copy_iter(conn, table, columns, rows):
    """COPY через STDIN"""
    with conn.cursor() as cur:
        sql = f"""
        COPY {table} ({", ".join(columns)})
        FROM STDIN WITH (FORMAT csv, DELIMITER E'\t', NULL '')
        """
        with io.StringIO() as f:
            for row in rows:
                f.write("\t".join(map(str, row)) + "\n")
            f.seek(0)
            cur.copy_expert(sql, f)
    conn.commit()



def exec_sql(conn: PGConnection, sql: str):
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


# -----------------------------
# Основной пайплайн
# -----------------------------

def create_schema(conn: PGConnection):
    # ускорение загрузки
    with conn.cursor() as cur:
        cur.execute("SET synchronous_commit = OFF;")
        cur.execute("SET maintenance_work_mem = '1GB';")
        cur.execute("SET work_mem = '64MB';")
        cur.execute("SET temp_buffers = '64MB';")
    conn.commit()

    for stmt in DDL:
        exec_sql(conn, stmt)


def truncate_all(conn: PGConnection):
    exec_sql(conn, "TRUNCATE TABLE \
        audit, metrics, logs, chat_messages, chats, notifications, messages, \
        transactions, invoices, shipments, payments, order_items, orders, \
        product_categories, products, categories, user_roles, roles, profiles, users \
        RESTART IDENTITY CASCADE;")


def build_indexes(conn: PGConnection):
    # Индексы создаём вне транзакции по одному — можно CONCURRENTLY, но тогда без транзакции.
    conn.set_session(autocommit=True)
    with conn.cursor() as cur:
        for stmt in INDEXES:
            cur.execute(stmt)
    conn.set_session(autocommit=False)


def load_all(conn: PGConnection, args):
    n_users = args.users
    n_categories = args.categories
    n_products = args.products
    n_orders = args.orders
    n_transactions = args.transactions
    n_messages = args.messages
    n_notifications = args.notifications
    n_chats = args.chats
    n_chat_messages = args.chat_messages
    n_logs = args.logs
    n_metrics = args.metrics
    n_audit = args.audit

    # Порядок загрузки учитывает внешние ключи
    print("→ roles")
    copy_iter(conn, "roles", ["name"], roles_rows())

    print("→ users")
    copy_iter(conn, "users", ["username", "email", "created_at"], users_rows(n_users))

    print("→ profiles")
    copy_iter(conn, "profiles", ["user_id","first_name","last_name","bio","birth_date","country","city"], profiles_rows(n_users))

    print("→ user_roles")
    copy_iter(conn, "user_roles", ["user_id","role_id"], user_roles_rows(n_users))

    print("→ categories")
    copy_iter(conn, "categories", ["name"], categories_rows(n_categories))

    print("→ products")
    copy_iter(conn, "products", ["name","description","price","stock","created_at"], products_rows(n_products))

    print("→ product_categories")
    copy_iter(conn, "product_categories", ["product_id","category_id"], product_categories_rows(n_products, n_categories))

    print("→ orders")
    copy_iter(conn, "orders", ["user_id","status","total","created_at"], orders_rows(n_orders, n_users))

    print("→ order_items")
    copy_iter(conn, "order_items", ["order_id","product_id","quantity","price"], order_items_rows(n_orders, n_products))

    print("→ payments")
    copy_iter(conn, "payments", ["order_id","amount","method","status","paid_at"], payments_rows(n_orders))

    print("→ shipments")
    copy_iter(conn, "shipments", ["order_id","address","status","shipped_at"], shipments_rows(n_orders))

    print("→ invoices")
    copy_iter(conn, "invoices", ["order_id","pdf","issued_at"], invoices_rows(n_orders))

    print("→ transactions")
    copy_iter(conn, "transactions", ["user_id","amount","currency","status","created_at"], transactions_rows(n_users, n_transactions))

    print("→ messages")
    copy_iter(conn, "messages", ["sender_id","receiver_id","body","created_at"], messages_rows(n_messages, n_users))

    print("→ notifications")
    copy_iter(conn, "notifications", ["user_id","type","content","seen","created_at"], notifications_rows(n_notifications, n_users))

    print("→ chats")
    copy_iter(conn, "chats", ["title","created_at"], chats_rows(n_chats))

    print("→ chat_messages")
    copy_iter(conn, "chat_messages", ["chat_id","user_id","body","created_at"], chat_messages_rows(n_chat_messages, n_chats, n_users))

    print("→ logs")
    copy_iter(conn, "logs", ["user_id","action","details","ip","created_at"], logs_rows(n_logs, n_users))

    print("→ metrics")
    copy_iter(conn, "metrics", ["name","value","collected_at"], metrics_rows(n_metrics))

    print("→ audit")
    copy_iter(conn, "audit", ["table_name","row_id","action","changed_at","user_id"], audit_rows(n_audit, n_users))


# -----------------------------
# CLI
# -----------------------------

def parse_args():
    p = argparse.ArgumentParser(description="Генератор большой БД PostgreSQL (20+ таблиц) через COPY")
    p.add_argument("--dsn", required=True, help="Строка подключения libpq, напр.: host=127.0.0.1 port=5432 dbname=default_db user=gen_user password=Passwd123")
    p.add_argument("--seed", type=int, default=42, help="Seed для воспроизводимости")
    p.add_argument("--truncate", action="store_true", help="Очистить все таблицы (TRUNCATE ... RESTART IDENTITY)")

    # Объёмы данных
    p.add_argument("--users", type=int, default=1_000_000)
    p.add_argument("--categories", type=int, default=2_000)
    p.add_argument("--products", type=int, default=200_000)
    p.add_argument("--orders", type=int, default=5_000_000)
    p.add_argument("--transactions", type=int, default=10_000_000)
    p.add_argument("--messages", type=int, default=10_000_000)
    p.add_argument("--notifications", type=int, default=5_000_000)
    p.add_argument("--chats", type=int, default=200_000)
    p.add_argument("--chat_messages", type=int, default=20_000_000)
    p.add_argument("--logs", type=int, default=50_000_000)
    p.add_argument("--metrics", type=int, default=2_000_000)
    p.add_argument("--audit", type=int, default=2_000_000)

    return p.parse_args()


def main():
    args = parse_args()
    random.seed(args.seed)
    Faker.seed(args.seed)

    print("Подключение к БД…")
    conn = psycopg2.connect(args.dsn)

    if args.truncate:
        print("TRUNCATE всех таблиц…")
        truncate_all(conn)

    print("Создание схемы и таблиц…")
    create_schema(conn)

    print("Загрузка данных COPY…")
    load_all(conn, args)

    print("Создание индексов…")
    build_indexes(conn)

    print("ANALYZE…")
    exec_sql(conn, "ANALYZE;")

    conn.close()
    print("✅ Готово!")


if __name__ == "__main__":
    main()


# -----------------------------
# python3 generate_big_pg_data.py \
#   --dsn "host=127.0.0.1 port=5432 dbname=default_db user=gen_user password=Passwd123" \
#   --truncate \
#   --users 1000000 --products 200000 --orders 20000000 --logs 100000000 \
#   --messages 30000000 --transactions 15000000 --chat_messages 30000000
# ----------------------------- 868