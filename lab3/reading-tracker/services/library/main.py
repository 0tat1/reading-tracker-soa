"""Library Service (Library Management, core-домен).
SYNC: при додаванні книги викликає Catalog Service (через Consul + circuit breaker).
ASYNC: усі library.* події пишуться в таблицю outbox у тій самій транзакції, що й зміна даних (Transactional Outbox);
окрема фонова задача (dispatcher) відправляє їх у RabbitMQ.
SAGA: завершення книги = розподілена транзакція (Library -> Saga Coordinator -> Analytics); Library виконує локальні
кроки і компенсацію (saga.completed / saga.compensate)."""
import asyncio, os, uuid
from contextlib import asynccontextmanager

from psycopg.types.json import Jsonb

from fastapi import FastAPI, Header, HTTPException, Request
from pydantic import BaseModel, Field

from common import db, infra

broker = infra.Broker()
catalog_breaker = infra.CircuitBreaker(failures=3, reset_timeout=10)
SCHEMA = """
CREATE TABLE IF NOT EXISTS libraries(user_id INT PRIMARY KEY, created_at TIMESTAMPTZ DEFAULT now());
CREATE TABLE IF NOT EXISTS items(
  id SERIAL PRIMARY KEY, user_id INT NOT NULL, book_id INT NOT NULL, title TEXT, author TEXT, genre TEXT,
  pages_total INT NOT NULL, pages_read INT NOT NULL DEFAULT 0, shelf TEXT NOT NULL DEFAULT 'ToRead',
  UNIQUE (user_id, book_id));
ALTER TABLE items ADD COLUMN IF NOT EXISTS saga_id TEXT;
CREATE TABLE IF NOT EXISTS reading_sessions(
  id SERIAL PRIMARY KEY, item_id INT NOT NULL, pages INT NOT NULL, logged_at TIMESTAMPTZ DEFAULT now());
-- Transactional Outbox: подія зберігається разом зі зміною даних, а в брокер її відправляє dispatcher
CREATE TABLE IF NOT EXISTS outbox(
  id SERIAL PRIMARY KEY, event_id TEXT UNIQUE NOT NULL, saga_id TEXT, event_type TEXT NOT NULL, payload JSONB NOT NULL,
  status TEXT NOT NULL DEFAULT 'new',            -- new -> sending -> sent | failed
  attempts INT NOT NULL DEFAULT 0, available_at TIMESTAMPTZ NOT NULL DEFAULT now(), locked_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ DEFAULT now(), sent_at TIMESTAMPTZ);
"""
OUTBOX_MAX_ATTEMPTS = int(os.getenv("OUTBOX_MAX_ATTEMPTS", "8"))


def outbox_add(c, key: str, data: dict, saga_id: str | None = None):
    """Запис події в outbox У ТІЙ САМІЙ транзакції (c — з'єднання відкритої транзакції)."""
    c.execute("INSERT INTO outbox(event_id,saga_id,event_type,payload) VALUES(%s,%s,%s,%s)",
              (str(uuid.uuid4()), saga_id, key, Jsonb(data)))


def claim_batch():
    """Атомарно «захоплює» до 20 подій (SKIP LOCKED — кілька реплік не беруть одну й ту саму подію).
    Також повертає в роботу події, що завісли у стані sending (репліка впала між захопленням і відправкою)."""
    return db.run("""UPDATE outbox SET status='sending', locked_at=now(), attempts=attempts+1 WHERE id IN (
                       SELECT id FROM outbox
                       WHERE (status='new' AND available_at <= now()) OR (status='sending' AND locked_at < now() - interval '30 seconds')
                       ORDER BY id LIMIT 20 FOR UPDATE SKIP LOCKED) RETURNING *""", fetch="all")


def mark_sent(row_id):
    db.run("UPDATE outbox SET status='sent', sent_at=now() WHERE id=%s", (row_id,))


def mark_retry(row_id, attempts):
    if attempts >= OUTBOX_MAX_ATTEMPTS:
        db.run("UPDATE outbox SET status='failed' WHERE id=%s", (row_id,))
    else:   # повторна спроба з наростаючою паузою
        db.run("UPDATE outbox SET status='new', available_at=now() + make_interval(secs => %s) WHERE id=%s", (attempts * 2, row_id))


async def outbox_dispatcher():
    """Фоновий relay: читає outbox і публікує в RabbitMQ. Якщо брокер недоступний — подія лишається в БД і піде пізніше."""
    while True:
        try:
            rows = await asyncio.to_thread(claim_batch)
            for r in rows:
                try:
                    await broker.publish(r["event_type"], r["payload"], event_id=r["event_id"])
                    await asyncio.to_thread(mark_sent, r["id"])
                    infra.log.info("[outbox] event %s (%s) marked as sent", r["event_id"], r["event_type"])
                except Exception as e:
                    infra.log.warning("[outbox] publish failed for %s (attempt %s): %s", r["event_id"], r["attempts"], e)
                    await asyncio.to_thread(mark_retry, r["id"], r["attempts"])
            await asyncio.sleep(0 if rows else 1)
        except Exception as e:
            infra.log.warning("[outbox] dispatcher error: %s", e)
            await asyncio.sleep(2)


def base_of(it: dict) -> dict:
    return {"user_id": it["user_id"], "item_id": it["id"], "book_id": it["book_id"], "title": it["title"],
            "author": it["author"], "genre": it["genre"]}


def on_user_registered_sync(event):
    db.run("INSERT INTO libraries(user_id) VALUES(%s) ON CONFLICT DO NOTHING", (event["data"]["user_id"],))


async def on_user_registered(event):
    await asyncio.to_thread(on_user_registered_sync, event)


def saga_completed_sync(event):
    """Крок SAGA «підтвердити»: Pending -> Completed + подія library.book-completed (через outbox). Ідемпотентно."""
    d = event["data"]
    with db.transaction() as c:
        it = c.execute("UPDATE items SET shelf='Completed' WHERE id=%s AND shelf='Pending' AND saga_id=%s RETURNING *",
                       (d["item_id"], d["saga_id"])).fetchone()
        if it:
            outbox_add(c, "library.book-completed", base_of(it), d["saga_id"])
            infra.log.info("[saga %s] book %s -> Completed", d["saga_id"], it["id"])


def saga_compensate_sync(event):
    """КОМПЕНСУЮЧА транзакція: Pending -> Reading (книга знову «в процесі»), подія library.completion-canceled."""
    d = event["data"]
    with db.transaction() as c:
        it = c.execute("UPDATE items SET shelf='Reading', saga_id=NULL WHERE id=%s AND shelf='Pending' AND saga_id=%s RETURNING *",
                       (d["item_id"], d["saga_id"])).fetchone()
        if it:
            outbox_add(c, "library.completion-canceled", base_of(it), d["saga_id"])
            infra.log.info("[saga %s] COMPENSATION: book %s rolled back to Reading", d["saga_id"], it["id"])


async def on_saga_completed(event):
    await asyncio.to_thread(saga_completed_sync, event)


async def on_saga_compensate(event):
    await asyncio.to_thread(saga_compensate_sync, event)


@asynccontextmanager
async def lifespan(app):
    db.init("library_db", SCHEMA)
    await broker.connect()
    await broker.consume("library.q", ["user.registered"], on_user_registered)
    await broker.consume("library.saga.q", ["saga.completed"], on_saga_completed)
    await broker.consume("library.compensate.q", ["saga.compensate"], on_saga_compensate)
    await infra.register_in_consul()
    task = asyncio.create_task(outbox_dispatcher())
    yield
    task.cancel()
    await infra.deregister_from_consul()
    await broker.close()


app = FastAPI(title="Library Service", lifespan=lifespan)


@app.middleware("http")
async def add_instance_header(request: Request, call_next):
    resp = await call_next(request)
    resp.headers["X-Instance"] = infra.INSTANCE
    return resp


class AddItem(BaseModel):
    book_id: int


class LogPages(BaseModel):
    pages: int = Field(gt=0)


def own_item(item_id: int, user_id: int) -> dict:
    it = db.run("SELECT * FROM items WHERE id=%s AND user_id=%s", (item_id, user_id), "one")
    if not it:
        raise HTTPException(404, "library item not found")
    return it


@app.get("/health")
def health():
    return {"status": "ok", "instance": infra.INSTANCE, "catalog_breaker": catalog_breaker.state}


@app.get("/items")
def list_items(x_user_id: int = Header()):
    return db.run("SELECT * FROM items WHERE user_id=%s ORDER BY id", (x_user_id,), "all")


def add_item_sync(uid, book):
    with db.transaction() as c:
        item = c.execute("INSERT INTO items(user_id,book_id,title,author,genre,pages_total) VALUES(%s,%s,%s,%s,%s,%s) RETURNING *",
                         (uid, book["id"], book["title"], book["author"], book["genre"], book["pages"])).fetchone()
        c.execute("INSERT INTO libraries(user_id) VALUES(%s) ON CONFLICT DO NOTHING", (uid,))
        outbox_add(c, "library.book-added", base_of(item))
    return item


@app.post("/items", status_code=201)
async def add_item(body: AddItem, x_user_id: int = Header()):
    if await db.arun("SELECT 1 FROM items WHERE user_id=%s AND book_id=%s", (x_user_id, body.book_id), "one"):
        raise HTTPException(409, "book is already in your library")          # інваріант: без дублікатів
    try:                                                                       # === синхронний виклик Catalog ===
        r = await infra.call_service(catalog_breaker, "catalog-service", "GET", f"/books/{body.book_id}")
    except infra.ServiceUnavailable as e:
        raise HTTPException(503, f"catalog unavailable: {e}")
    if r.status_code == 404:
        raise HTTPException(404, "book not found in catalog")
    book = r.json()
    item = await asyncio.to_thread(add_item_sync, x_user_id, book)
    return {**item, "catalog_instance": book.get("instance"), "instance": infra.INSTANCE}


def start_saga(c, it: dict) -> str:
    """Локальна транзакція №1 SAGA: книга -> Pending + подія library.saga-started в outbox (атомарно)."""
    saga_id = str(uuid.uuid4())
    c.execute("UPDATE items SET shelf='Pending', saga_id=%s WHERE id=%s", (saga_id, it["id"]))
    outbox_add(c, "library.saga-started", {**base_of(it), "saga_id": saga_id, "pages_total": it["pages_total"]}, saga_id)
    return saga_id


def log_pages_sync(item_id, uid, req_pages):
    with db.transaction() as c:
        it = c.execute("SELECT * FROM items WHERE id=%s AND user_id=%s FOR UPDATE", (item_id, uid)).fetchone()   # песимістичне блокування рядка
        if not it:
            raise HTTPException(404, "library item not found")
        if it["shelf"] in ("Completed", "Dropped"):
            raise HTTPException(409, f"book is on the {it['shelf']} shelf")
        if it["shelf"] == "Pending":
            raise HTTPException(409, "completion of this book is in progress (saga)")
        pages = min(req_pages, it["pages_total"] - it["pages_read"])           # 0 <= pages_read <= pages_total
        if pages <= 0:
            raise HTTPException(409, "all pages are read already - call POST /items/{id}/complete")
        new_read = it["pages_read"] + pages
        c.execute("INSERT INTO reading_sessions(item_id,pages) VALUES(%s,%s)", (item_id, pages))
        it = c.execute("UPDATE items SET pages_read=%s, shelf='Reading' WHERE id=%s RETURNING *", (new_read, item_id)).fetchone()
        outbox_add(c, "library.pages-logged", {**base_of(it), "pages": pages})
        saga_id = start_saga(c, it) if new_read >= it["pages_total"] else None      # дочитав -> стартує SAGA
        it = c.execute("SELECT * FROM items WHERE id=%s", (item_id,)).fetchone()
    return it, saga_id


@app.post("/items/{item_id}/pages")
async def log_pages(item_id: int, body: LogPages, x_user_id: int = Header()):
    it, saga_id = await asyncio.to_thread(log_pages_sync, item_id, x_user_id, body.pages)
    return {**it, "saga_id": saga_id, "instance": infra.INSTANCE}


def complete_sync(item_id, uid):
    with db.transaction() as c:
        it = c.execute("SELECT * FROM items WHERE id=%s AND user_id=%s FOR UPDATE", (item_id, uid)).fetchone()
        if not it:
            raise HTTPException(404, "library item not found")
        if it["shelf"] != "Reading" or it["pages_read"] < it["pages_total"]:
            raise HTTPException(409, "only a fully read book on the Reading shelf can be completed")
        saga_id = start_saga(c, it)
        it = c.execute("SELECT * FROM items WHERE id=%s", (item_id,)).fetchone()
    return it, saga_id


@app.post("/items/{item_id}/complete")
async def complete(item_id: int, x_user_id: int = Header()):
    """Повторний запуск SAGA завершення (наприклад, після відкату)."""
    it, saga_id = await asyncio.to_thread(complete_sync, item_id, x_user_id)
    return {**it, "saga_id": saga_id, "instance": infra.INSTANCE}


def drop_sync(item_id, uid):
    with db.transaction() as c:
        it = c.execute("SELECT * FROM items WHERE id=%s AND user_id=%s FOR UPDATE", (item_id, uid)).fetchone()
        if not it:
            raise HTTPException(404, "library item not found")
        if it["shelf"] in ("Completed", "Pending"):
            raise HTTPException(409, f"a book on the {it['shelf']} shelf cannot be dropped")
        it = c.execute("UPDATE items SET shelf='Dropped' WHERE id=%s RETURNING *", (item_id,)).fetchone()
        outbox_add(c, "library.book-dropped", base_of(it))
    return it


@app.post("/items/{item_id}/drop")
async def drop(item_id: int, x_user_id: int = Header()):
    return await asyncio.to_thread(drop_sync, item_id, x_user_id)


def remove_sync(item_id, uid):
    with db.transaction() as c:
        it = c.execute("SELECT * FROM items WHERE id=%s AND user_id=%s FOR UPDATE", (item_id, uid)).fetchone()
        if not it:
            raise HTTPException(404, "library item not found")
        if it["shelf"] == "Pending":
            raise HTTPException(409, "completion of this book is in progress (saga)")
        c.execute("DELETE FROM items WHERE id=%s", (item_id,))
        outbox_add(c, "library.book-removed", base_of(it))


@app.delete("/items/{item_id}")
async def remove(item_id: int, x_user_id: int = Header()):
    await asyncio.to_thread(remove_sync, item_id, x_user_id)
    return {"removed": item_id}


@app.get("/outbox")
def outbox_view(limit: int = 20):
    """Для демонстрації: стан таблиці outbox (останні події)."""
    return db.run("SELECT id, event_type, saga_id, status, attempts, created_at, sent_at FROM outbox ORDER BY id DESC LIMIT %s", (limit,), "all")
