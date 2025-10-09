#!/usr/bin/env python3
"""
pg_prod_simulator.py
Имитация работы продовой Postgres БД:
- создаёт таблицы, первоначально заполняет
- выполняет mix операций: select, insert, update, delete, upsert, transactions, joins
- позволяет регулировать нагрузку (concurrency, ops_per_sec, ratios)
- подключается к удалённой БД
"""

# pip install psycopg2-binary
# python3 pg_prod_simulator.py \
#   --host 192.0.2.10 --port 5432 --user tester --password secret \
#   --dbname prod_test --concurrency 8 --ops_per_sec 200 --duration 600

import argparse
import random
import string
import time
import threading
import signal
import sys
import math
from contextlib import contextmanager
from collections import defaultdict
from datetime import datetime, timedelta

import psycopg2
from psycopg2 import sql
from psycopg2.pool import ThreadedConnectionPool

# --------- Configuration Defaults ---------
DEFAULT_TABLE_COUNT = 3
DEFAULT_ROWS_PER_TABLE = 1000
DEFAULT_BATCH_INSERT = 100
DEFAULT_CONCURRENCY = 4
DEFAULT_OPS_PER_SEC = 100
DEFAULT_DURATION = 300  # seconds
DEFAULT_RATIO = {
    'select_point': 0.35,
    'select_range': 0.10,
    'insert': 0.20,
    'batch_insert': 0.05,
    'update': 0.15,
    'delete': 0.02,
    'upsert': 0.05,
    'transaction': 0.03,
    'join': 0.05
}

STOP = threading.Event()

# --------- Utility functions ---------
def rand_string(n=12):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=n))

def now_ts():
    return datetime.utcnow().isoformat()

# --------- DB helper/wrapper ---------
class DBWorker:
    def __init__(self, pool, table_names, batch_insert=100):
        self.pool = pool
        self.table_names = table_names
        self.batch_insert = batch_insert

    @contextmanager
    def conn(self):
        conn = self.pool.getconn()
        try:
            yield conn
        finally:
            self.pool.putconn(conn)

    def setup_schema(self):
        """
        Create simple schema:
        table_i (id serial primary key, key text, data text, value int, created timestamptz)
        Also create a smaller lookup table for joins.
        """
        with self.conn() as conn:
            cur = conn.cursor()
            # extension for pgcrypto uuid if needed, but keep simple
            for t in self.table_names:
                cur.execute(sql.SQL("""
                    CREATE TABLE IF NOT EXISTS {tbl} (
                        id SERIAL PRIMARY KEY,
                        key_text TEXT NOT NULL,
                        data TEXT,
                        value INT,
                        created TIMESTAMP WITH TIME ZONE DEFAULT now()
                    );
                """).format(tbl=sql.Identifier(t)))
                # index on key_text and value for selects
                cur.execute(sql.SQL("CREATE INDEX IF NOT EXISTS {idx} ON {tbl} (key_text);")
                            .format(idx=sql.Identifier(f"{t}_key_idx"), tbl=sql.Identifier(t)))
                cur.execute(sql.SQL("CREATE INDEX IF NOT EXISTS {idx2} ON {tbl} (value);")
                            .format(idx2=sql.Identifier(f"{t}_value_idx"), tbl=sql.Identifier(t)))
            # helper lookup table for join tests
            cur.execute("""
                CREATE TABLE IF NOT EXISTS lookup_table (
                    id SERIAL PRIMARY KEY,
                    name TEXT UNIQUE,
                    meta TEXT
                );
            """)
            conn.commit()
            cur.close()

    def seed_initial_data(self, rows_per_table=1000):
        with self.conn() as conn:
            cur = conn.cursor()
            # seed lookup first
            cur.execute("SELECT count(*) FROM lookup_table;")
            cnt = cur.fetchone()[0]
            if cnt < 100:
                batch = []
                for i in range(200):
                    batch.append((f"name_{i}", f"meta_{rand_string(8)}"))
                args_str = b','.join(cur.mogrify("(%s,%s)", x) for x in batch)
                cur.execute(b"INSERT INTO lookup_table (name, meta) VALUES " + args_str +
                            b" ON CONFLICT (name) DO NOTHING;")
            # seed main tables
            for t in self.table_names:
                cur.execute(sql.SQL("SELECT count(*) FROM {tbl};").format(tbl=sql.Identifier(t)))
                existing = cur.fetchone()[0]
                to_insert = max(0, rows_per_table - existing)
                if to_insert <= 0:
                    continue
                batch_size = 200
                while to_insert > 0:
                    cur_batch = min(batch_size, to_insert)
                    batch = []
                    for _ in range(cur_batch):
                        key = f"k_{random.randint(1, rows_per_table*10)}"
                        data = rand_string(64)
                        value = random.randint(1, rows_per_table*10)
                        batch.append((key, data, value))
                    args_str = b','.join(cur.mogrify("(%s,%s,%s)", x) for x in batch)
                    cur.execute(sql.SQL("INSERT INTO {tbl} (key_text, data, value) VALUES ") +
                                sql.SQL(args_str.decode('latin1')) + sql.SQL(" ON CONFLICT DO NOTHING;")
                                .format(tbl=sql.Identifier(t)))
                    to_insert -= cur_batch
            conn.commit()
            cur.close()

    # --------- Operation implementations ---------
    def op_select_point(self):
        t = random.choice(self.table_names)
        # pick random id range based on estimated counts
        with self.conn() as conn:
            cur = conn.cursor()
            # try by id
            cur.execute(sql.SQL("SELECT id FROM {tbl} ORDER BY random() LIMIT 1").format(tbl=sql.Identifier(t)))
            row = cur.fetchone()
            if row:
                cur.execute(sql.SQL("SELECT id, key_text, data, value FROM {tbl} WHERE id = %s").format(tbl=sql.Identifier(t)), (row[0],))
                _ = cur.fetchone()
            else:
                # fallback: random key_text
                key = f"k_{random.randint(1, 10000)}"
                cur.execute(sql.SQL("SELECT id FROM {tbl} WHERE key_text = %s LIMIT 1").format(tbl=sql.Identifier(t)), (key,))
                _ = cur.fetchone()
            cur.close()

    def op_select_range(self):
        t = random.choice(self.table_names)
        with self.conn() as conn:
            cur = conn.cursor()
            low = random.randint(1, 1000)
            high = low + random.randint(1, 200)
            cur.execute(sql.SQL("SELECT id,key_text,value FROM {tbl} WHERE value BETWEEN %s AND %s LIMIT 200")
                        .format(tbl=sql.Identifier(t)), (low, high))
            _ = cur.fetchall()
            cur.close()

    def op_insert_single(self):
        t = random.choice(self.table_names)
        with self.conn() as conn:
            cur = conn.cursor()
            cur.execute(sql.SQL("INSERT INTO {tbl} (key_text, data, value) VALUES (%s,%s,%s) RETURNING id")
                        .format(tbl=sql.Identifier(t)), (f"k_{random.randint(1,1000000)}", rand_string(128), random.randint(1,1000000)))
            _ = cur.fetchone()
            conn.commit()
            cur.close()

    def op_batch_insert(self, batch_size=None):
        batch_size = batch_size or self.batch_insert
        t = random.choice(self.table_names)
        with self.conn() as conn:
            cur = conn.cursor()
            batch = []
            for _ in range(batch_size):
                batch.append((f"k_{random.randint(1,1000000)}", rand_string(64), random.randint(1,1000000)))
            args_str = b','.join(cur.mogrify("(%s,%s,%s)", x) for x in batch)
            # use plain execute with bytes; psycopg2 will accept
            cur.execute(sql.SQL("INSERT INTO {tbl} (key_text, data, value) VALUES ") +
                        sql.SQL(args_str.decode('latin1')) + sql.SQL(" ON CONFLICT DO NOTHING;")
                        .format(tbl=sql.Identifier(t)))
            conn.commit()
            cur.close()

    def op_update(self):
        t = random.choice(self.table_names)
        with self.conn() as conn:
            cur = conn.cursor()
            # update a random row's data/value
            cur.execute(sql.SQL("SELECT id FROM {tbl} ORDER BY random() LIMIT 1").format(tbl=sql.Identifier(t)))
            row = cur.fetchone()
            if row:
                new_data = rand_string(80)
                new_val = random.randint(1, 1000000)
                cur.execute(sql.SQL("UPDATE {tbl} SET data = %s, value = %s WHERE id = %s")
                            .format(tbl=sql.Identifier(t)), (new_data, new_val, row[0]))
                conn.commit()
            cur.close()

    def op_delete(self):
        t = random.choice(self.table_names)
        with self.conn() as conn:
            cur = conn.cursor()
            # delete some old-ish rows to simulate churn
            cur.execute(sql.SQL("DELETE FROM {tbl} WHERE id IN (SELECT id FROM {tbl} ORDER BY random() LIMIT 1)")
                        .format(tbl=sql.Identifier(t)))
            conn.commit()
            cur.close()

    def op_upsert(self):
        t = random.choice(self.table_names)
        key = f"unique_k_{random.randint(1,2000)}"
        with self.conn() as conn:
            cur = conn.cursor()
            cur.execute(sql.SQL("""
                INSERT INTO {tbl} (key_text, data, value) VALUES (%s,%s,%s)
                ON CONFLICT (key_text) DO UPDATE SET data = EXCLUDED.data, value = EXCLUDED.value
            """).format(tbl=sql.Identifier(t)), (key, rand_string(64), random.randint(1,1000000)))
            conn.commit()
            cur.close()

    def op_transaction(self):
        """
        Simulate a small transaction with multiple statements:
        read -> condition -> update/insert
        """
        t = random.choice(self.table_names)
        with self.conn() as conn:
            cur = conn.cursor()
            try:
                cur.execute("BEGIN;")
                cur.execute(sql.SQL("SELECT id, value FROM {tbl} ORDER BY random() LIMIT 1 FOR UPDATE").format(tbl=sql.Identifier(t)))
                r = cur.fetchone()
                if r:
                    # update
                    cur.execute(sql.SQL("UPDATE {tbl} SET value = value + 1 WHERE id = %s").format(tbl=sql.Identifier(t)), (r[0],))
                else:
                    cur.execute(sql.SQL("INSERT INTO {tbl} (key_text, data, value) VALUES (%s,%s,%s)")
                                .format(tbl=sql.Identifier(t)), (f"k_{random.randint(1,1000000)}", rand_string(40), 1))
                cur.execute("COMMIT;")
            except Exception:
                cur.execute("ROLLBACK;")
                raise
            finally:
                cur.close()

    def op_join(self):
        t = random.choice(self.table_names)
        with self.conn() as conn:
            cur = conn.cursor()
            # simple join with lookup_table (index on name)
            cur.execute(sql.SQL("""
                SELECT m.id, m.key_text, l.name, l.meta
                FROM {tbl} m
                JOIN lookup_table l ON (l.id = (m.id % 200) + 1)
                WHERE m.value BETWEEN %s AND %s
                LIMIT 100;
            """).format(tbl=sql.Identifier(t)), (random.randint(1,10000), random.randint(10001,20000)))
            _ = cur.fetchall()
            cur.close()


# --------- Worker thread that picks ops according to ratios and pace ---------
class OpRunner(threading.Thread):
    def __init__(self, name, dbworker, ratios, ops_per_sec, stats):
        super().__init__(daemon=True)
        self.name = name
        self.dbworker = dbworker
        self.ratios = ratios
        self.ops_per_sec = ops_per_sec  # target operations per second for this worker (can be fractional)
        self.stats = stats
        self.ops_map = {
            'select_point': dbworker.op_select_point,
            'select_range': dbworker.op_select_range,
            'insert': dbworker.op_insert_single,
            'batch_insert': dbworker.op_batch_insert,
            'update': dbworker.op_update,
            'delete': dbworker.op_delete,
            'upsert': dbworker.op_upsert,
            'transaction': dbworker.op_transaction,
            'join': dbworker.op_join
        }
        # build cumulative distribution
        keys = list(ratios.keys())
        weights = [ratios[k] for k in keys]
        total = sum(weights)
        if total <= 0:
            raise ValueError("Ratios sum must be > 0")
        self.cdf = []
        acc = 0.0
        for k in keys:
            acc += ratios[k] / total
            self.cdf.append((acc, k))

    def choose_op(self):
        r = random.random()
        for thresh, k in self.cdf:
            if r <= thresh:
                return self.ops_map[k]
        return self.ops_map[self.cdf[-1][1]]

    def run(self):
        sleep_interval = 0.0
        if self.ops_per_sec > 0:
            sleep_interval = 1.0 / self.ops_per_sec
        last = time.time()
        while not STOP.is_set():
            try:
                op = self.choose_op()
                start = time.time()
                op()
                dur = time.time() - start
                # record stats
                self.stats['ops'] += 1
                self.stats['by_type'][op.__name__] += 1
                self.stats['time_total'] += dur
            except Exception as e:
                self.stats['errors'] += 1
                # keep running - log occasional
                if self.stats['errors'] % 10 == 1:
                    print(f"[{now_ts()}] Worker {self.name} error: {repr(e)}", file=sys.stderr)
            # pacing
            if sleep_interval > 0:
                # sleep but wake sooner on STOP
                end = last + sleep_interval
                to_sleep = end - time.time()
                if to_sleep > 0:
                    STOP.wait(to_sleep)
                last = time.time()
            else:
                # yield tiny bit
                STOP.wait(0.001)


# --------- Top-level controller / main ---------
def main():
    parser = argparse.ArgumentParser(description="Postgres prod workload simulator")
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", type=int, default=5432)
    parser.add_argument("--user", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--dbname", required=True)
    parser.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY, help="number of worker threads")
    parser.add_argument("--ops_per_sec", type=int, default=DEFAULT_OPS_PER_SEC, help="total ops per second (approx)")
    parser.add_argument("--duration", type=int, default=DEFAULT_DURATION, help="duration seconds")
    parser.add_argument("--table_count", type=int, default=DEFAULT_TABLE_COUNT)
    parser.add_argument("--rows_per_table", type=int, default=DEFAULT_ROWS_PER_TABLE)
    parser.add_argument("--batch_insert", type=int, default=DEFAULT_BATCH_INSERT)
    parser.add_argument("--seed", action="store_true", help="seed initial data")
    parser.add_argument("--no_create", action="store_true", help="do not create schema (assume exists)")
    # ratios: user may override individual ratios as JSON-like "select_point=0.4,insert=0.3,update=0.2,..."
    parser.add_argument("--ratios", default="", help="comma-separated op=weight pairs to override defaults")
    args = parser.parse_args()

    # build final ratios
    ratios = DEFAULT_RATIO.copy()
    if args.ratios:
        for part in args.ratios.split(","):
            if not part.strip():
                continue
            if "=" not in part:
                continue
            k, v = part.split("=", 1)
            k = k.strip()
            try:
                v = float(v)
            except:
                continue
            if k in ratios:
                ratios[k] = v

    # distribute ops_per_sec among workers evenly
    per_worker_ops = float(args.ops_per_sec) / max(1, args.concurrency)

    # table names
    table_names = [f"prod_sim_{i+1}" for i in range(args.table_count)]

    # create DB pool
    pool = ThreadedConnectionPool(1, max(2, args.concurrency + 2),
                                  host=args.host, port=args.port, user=args.user,
                                  password=args.password, dbname=args.dbname)

    dbworker = DBWorker(pool, table_names, batch_insert=args.batch_insert)

    if not args.no_create:
        print(f"[{now_ts()}] Creating schema and indexes...")
        dbworker.setup_schema()
    if args.seed:
        print(f"[{now_ts()}] Seeding initial data ({args.rows_per_table} rows per table)...")
        dbworker.seed_initial_data(rows_per_table=args.rows_per_table)
        print(f"[{now_ts()}] Seeding done.")

    # stats container (shared)
    stats = defaultdict(int)
    stats['by_type'] = defaultdict(int)
    stats['time_total'] = 0.0

    runners = []
    for i in range(args.concurrency):
        r = OpRunner(f"w{i+1}", dbworker, ratios, per_worker_ops, stats)
        runners.append(r)

    def sigint_handler(signum, frame):
        print(f"\n[{now_ts()}] Received stop signal, shutting down gracefully...")
        STOP.set()

    signal.signal(signal.SIGINT, sigint_handler)
    signal.signal(signal.SIGTERM, sigint_handler)

    print(f"[{now_ts()}] Starting {len(runners)} workers, target total ops/sec ~= {args.ops_per_sec}")
    for r in runners:
        r.start()

    start_time = time.time()
    next_report = start_time + 5
    end_time = start_time + args.duration if args.duration > 0 else float('inf')

    last_ops = 0
    try:
        while time.time() < end_time and not STOP.is_set():
            now = time.time()
            if now >= next_report:
                elapsed = now - start_time
                total_ops = stats['ops']
                ops_per_sec = (total_ops / elapsed) if elapsed > 0 else 0
                avg_latency = (stats['time_total'] / total_ops) if total_ops > 0 else 0
                print(f"[{now_ts()}] elapsed={int(elapsed)}s total_ops={total_ops} ops/s={ops_per_sec:.2f} avg_latency={avg_latency*1000:.2f}ms errors={stats['errors']}")
                # show breakdown
                for k, v in sorted(stats['by_type'].items(), key=lambda x: -x[1])[:10]:
                    print(f"   {k}: {v}")
                next_report = now + 5
            time.sleep(0.5)
    except KeyboardInterrupt:
        STOP.set()

    # signal stop and wait threads
    STOP.set()
    print(f"[{now_ts()}] Waiting for workers to finish...")
    for r in runners:
        r.join(timeout=5)

    total_elapsed = time.time() - start_time
    total_ops = stats['ops']
    print(f"[{now_ts()}] Finished. elapsed={int(total_elapsed)}s total_ops={total_ops} ops/s={(total_ops/total_elapsed if total_elapsed>0 else 0):.2f} errors={stats['errors']}")
    pool.closeall()


if __name__ == "__main__":
    main()
