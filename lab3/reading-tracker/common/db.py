"""Тонка обгортка над PostgreSQL. Кожен сервіс працює зі СВОЄЮ базою (database per service)."""
import asyncio, logging, os, time
import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

log = logging.getLogger("db")
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")
_pool: ConnectionPool | None = None


def ensure_database(db_name: str):
    """Створює БД сервісу, якщо її ще немає (потрібно, коли volume Postgres уже існує і init.sql не виконується)."""
    with psycopg.connect(f"host={DB_HOST} dbname=postgres user={DB_USER} password={DB_PASSWORD}", autocommit=True) as c:
        if not c.execute("SELECT 1 FROM pg_database WHERE datname=%s", (db_name,)).fetchone():
            try:
                c.execute(f'CREATE DATABASE "{db_name}"')
            except psycopg.errors.DuplicateDatabase:
                pass


def init(db_name: str, schema_sql: str):
    global _pool
    conninfo = f"host={DB_HOST} dbname={db_name} user={DB_USER} password={DB_PASSWORD}"
    for attempt in range(1, 61):
        try:
            ensure_database(db_name)
            _pool = ConnectionPool(conninfo, min_size=1, max_size=5, kwargs={"row_factory": dict_row}, open=True)
            _pool.wait(timeout=5)
            with _pool.connection() as c:
                c.execute("SELECT pg_advisory_xact_lock(42)")       # щоб репліки не створювали схему одночасно
                c.execute(schema_sql)
            log.info("database %s ready", db_name)
            return
        except Exception as e:
            log.warning("database not ready (%s/60): %s", attempt, e)
            time.sleep(2)
    raise RuntimeError("database is not available")


def run(sql: str, params=(), fetch: str | None = None):
    """fetch: None | 'one' | 'all'"""
    with _pool.connection() as c:
        cur = c.execute(sql, params)
        if fetch == "one":
            return cur.fetchone()
        if fetch == "all":
            return cur.fetchall()


async def arun(sql, params=(), fetch=None):
    return await asyncio.to_thread(run, sql, params, fetch)


def transaction():
    return _pool.connection()
