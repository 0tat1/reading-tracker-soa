"""Saga Coordinator (оркестратор SAGA «Завершення книги»).
Крок 1 (Library): книга -> Pending, подія library.saga-started (через outbox)
Крок 2 (Analytics): команда saga.process-completion -> відповідь analytics.completion-processed
Успіх:  saga.completed  -> Library: Pending -> Completed
Збій:   повідомлення потрапило в DLQ (saga.dlq) АБО спрацював таймаут -> saga.compensate -> Library: Pending -> Reading,
        Analytics: прибирає свій запис.
Журнал SAGA: таблиці saga_log (поточний стан) і saga_steps (журнал кроків)."""
import asyncio, os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Header, HTTPException, Request
from psycopg.types.json import Jsonb

from common import db, infra

broker = infra.Broker()
SAGA_TIMEOUT = int(os.getenv("SAGA_TIMEOUT", "30"))
SCHEMA = """
CREATE TABLE IF NOT EXISTS saga_log(
  saga_id TEXT PRIMARY KEY, state TEXT NOT NULL,            -- started -> sent -> processed | compensated
  user_id INT, item_id INT, payload JSONB, reason TEXT,
  started_at TIMESTAMPTZ DEFAULT now(), updated_at TIMESTAMPTZ DEFAULT now());
CREATE TABLE IF NOT EXISTS saga_steps(id SERIAL PRIMARY KEY, saga_id TEXT, step TEXT, at TIMESTAMPTZ DEFAULT now());
"""


def step(c, saga_id, text):
    c.execute("INSERT INTO saga_steps(saga_id,step) VALUES(%s,%s)", (saga_id, text))
    infra.log.info("[SAGA %s] %s", saga_id, text)


# ---- 1. Library повідомила про старт -> зберігаємо в журнал і відправляємо команду в Analytics
def on_started_sync(d):
    with db.transaction() as c:
        new = c.execute("INSERT INTO saga_log(saga_id,state,user_id,item_id,payload) VALUES(%s,'started',%s,%s,%s) "
                        "ON CONFLICT DO NOTHING RETURNING saga_id", (d["saga_id"], d["user_id"], d["item_id"], Jsonb(d))).fetchone()
        if new:
            step(c, d["saga_id"], "SAGA started (Library: book -> Pending)")
        return bool(new)


async def on_started(event):
    d = event["data"]
    await asyncio.to_thread(on_started_sync, d)
    row = await db.arun("SELECT state FROM saga_log WHERE saga_id=%s", (d["saga_id"],), "one")
    if row["state"] != "started":
        return
    await broker.publish("saga.process-completion", d)                       # команда для Analytics
    await db.arun("UPDATE saga_log SET state='sent', updated_at=now() WHERE saga_id=%s AND state='started'", (d["saga_id"],))
    await db.arun("INSERT INTO saga_steps(saga_id,step) VALUES(%s,'command sent to Analytics')", (d["saga_id"],))


# ---- 2. Analytics підтвердила -> SAGA успішна
async def on_processed(event):
    sid = event["data"]["saga_id"]
    row = await db.arun("SELECT * FROM saga_log WHERE saga_id=%s", (sid,), "one")
    if not row or row["state"] != "sent":
        infra.log.info("[SAGA %s] late/duplicate confirmation ignored (state=%s)", sid, row and row["state"])
        return
    await broker.publish("saga.completed", row["payload"])
    await db.arun("UPDATE saga_log SET state='processed', updated_at=now() WHERE saga_id=%s AND state='sent'", (sid,))
    await db.arun("INSERT INTO saga_steps(saga_id,step) VALUES(%s,'confirmed by Analytics -> saga.completed')", (sid,))
    infra.log.info("[SAGA %s] marked as processed", sid)


# ---- 3. Компенсація: збій у DLQ або таймаут
async def compensate(sid, reason):
    row = await db.arun("SELECT * FROM saga_log WHERE saga_id=%s", (sid,), "one")
    if not row or row["state"] not in ("started", "sent"):
        infra.log.info("[SAGA %s] compensation skipped (state=%s)", sid, row and row["state"])
        return
    await broker.publish("saga.compensate", row["payload"])                  # спершу команда, потім стан: при збої повторимо
    await db.arun("UPDATE saga_log SET state='compensated', reason=%s, updated_at=now() WHERE saga_id=%s", (reason, sid))
    await db.arun("INSERT INTO saga_steps(saga_id,step) VALUES(%s,%s)", (sid, f"{reason} -> saga.compensate published"))
    infra.log.info("[SAGA %s] %s -> performing rollback (compensation)", sid, reason)


async def on_dead_letter(event):
    if event["type"] != "saga.process-completion":
        infra.log.warning("[SAGA] dead letter of type %s ignored", event["type"])
        return
    sid = event["data"]["saga_id"]
    infra.log.info("[SAGA] DLQ detected failed event for saga %s", sid)
    await compensate(sid, "DLQ")


async def timeout_watcher():
    """Фоновий цикл: якщо SAGA надто довго без підтвердження — компенсуємо (таймаут)."""
    while True:
        await asyncio.sleep(3)
        try:
            rows = await db.arun("SELECT saga_id FROM saga_log WHERE state IN ('started','sent') "
                                 "AND updated_at < now() - make_interval(secs => %s)", (SAGA_TIMEOUT,), "all")
            for r in rows:
                infra.log.info("[SAGA] Timeout for saga %s", r["saga_id"])
                await compensate(r["saga_id"], "TIMEOUT")
        except Exception as e:
            infra.log.warning("[SAGA] watcher error: %s", e)


@asynccontextmanager
async def lifespan(app):
    db.init("saga_db", SCHEMA)
    await broker.connect()
    await broker.consume("saga.q", ["library.saga-started"], on_started)
    await broker.consume("saga.confirm.q", ["analytics.completion-processed"], on_processed)
    await broker.consume("saga.dlq", ["saga.failed"], on_dead_letter, exchange="dlx")    # Dead Letter Queue
    await infra.register_in_consul()
    task = asyncio.create_task(timeout_watcher())
    yield
    task.cancel()
    await infra.deregister_from_consul()
    await broker.close()


app = FastAPI(title="Saga Coordinator", lifespan=lifespan)


@app.middleware("http")
async def add_instance_header(request: Request, call_next):
    resp = await call_next(request)
    resp.headers["X-Instance"] = infra.INSTANCE
    return resp


@app.get("/health")
def health():
    return {"status": "ok", "instance": infra.INSTANCE, "timeout_s": SAGA_TIMEOUT}


@app.get("/sagas")
def sagas(x_user_id: int = Header()):
    return db.run("SELECT saga_id, state, item_id, reason, started_at, updated_at FROM saga_log WHERE user_id=%s ORDER BY started_at DESC LIMIT 50",
                  (x_user_id,), "all")


@app.get("/sagas/{saga_id}")
def saga(saga_id: str, x_user_id: int = Header()):
    s = db.run("SELECT saga_id, state, item_id, reason, started_at, updated_at FROM saga_log WHERE saga_id=%s AND user_id=%s",
               (saga_id, x_user_id), "one")
    if not s:
        raise HTTPException(404, "saga not found")
    s["steps"] = db.run("SELECT step, at FROM saga_steps WHERE saga_id=%s ORDER BY id", (saga_id,), "all")
    return s
