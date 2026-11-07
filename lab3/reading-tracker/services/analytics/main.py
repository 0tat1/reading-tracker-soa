"""Analytics Service (Analytics & Stats) = сервіс-консюмер + API.
Споживає library.*, rating.* і scheduler.month-ended. Обробка ідемпотентна: Transactional Inbox (таблиця inbox).
Бере участь у SAGA завершення книги: виконує команду saga.process-completion, а при saga.compensate скасовує свій крок.
Збій для демонстрації: SAGA_FAIL_EVERY=N -> кожна N-та сага завжди падає (після retries повідомлення йде в DLQ)."""
import asyncio, os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Header, Request
from psycopg.types.json import Jsonb

from common import db, infra

broker = infra.Broker()
SCHEMA = """
-- Transactional Inbox: кожне отримане повідомлення реєструється за message_id; повтор = дублікат (лічильник duplicates)
CREATE TABLE IF NOT EXISTS inbox(message_id TEXT PRIMARY KEY, event_type TEXT, saga_id TEXT, payload JSONB,
  status TEXT NOT NULL DEFAULT 'processed', duplicates INT NOT NULL DEFAULT 0, received_at TIMESTAMPTZ DEFAULT now());
CREATE TABLE IF NOT EXISTS genre_completions(saga_id TEXT PRIMARY KEY, user_id INT, book_id INT, genre TEXT, pages_total INT, month TEXT);
CREATE TABLE IF NOT EXISTS monthly_pages(user_id INT, month TEXT, genre TEXT, pages INT NOT NULL DEFAULT 0, PRIMARY KEY(user_id,month,genre));
CREATE TABLE IF NOT EXISTS history(id SERIAL PRIMARY KEY, user_id INT, action TEXT, book_id INT, title TEXT, at TIMESTAMPTZ DEFAULT now());
CREATE TABLE IF NOT EXISTS rating_stats(target_type TEXT, target TEXT, n INT NOT NULL DEFAULT 0, total INT NOT NULL DEFAULT 0, PRIMARY KEY(target_type,target));
CREATE TABLE IF NOT EXISTS monthly_reports(month TEXT, user_id INT, total_pages INT, by_genre JSONB, PRIMARY KEY(month,user_id));
"""


async def handle(event):
    """Один виклик = одна транзакція: або подію оброблено повністю, або (дубль) пропущено."""
    await asyncio.to_thread(process, event)


def process(event):
    t, d = event["type"], event["data"]
    month = event["occurred_at"][:7]
    with db.transaction() as c:
        if not inbox_register(c, event):
            infra.log.info("[inbox] duplicate event %s ignored", event["event_id"])       # ідемпотентність
            return
        if t == "library.pages-logged":
            c.execute("INSERT INTO monthly_pages(user_id,month,genre,pages) VALUES(%s,%s,%s,%s) "
                      "ON CONFLICT (user_id,month,genre) DO UPDATE SET pages = monthly_pages.pages + EXCLUDED.pages",
                      (d["user_id"], month, d["genre"], d["pages"]))
        elif t in ("library.book-added", "library.book-dropped", "library.book-removed", "library.book-completed",
                   "library.completion-canceled"):
            action = t.split("-", 1)[1].replace("-", "_")                          # added / dropped / removed / completed
            c.execute("INSERT INTO history(user_id,action,book_id,title) VALUES(%s,%s,%s,%s)",
                      (d["user_id"], action, d["book_id"], d.get("title")))
        elif t == "rating.avg-rating-updated":
            delta_n = 0 if d["previous_score"] else 1
            delta_total = d["score"] - (d["previous_score"] or 0)
            for typ, target in (("author", d["author"]), ("genre", d["genre"])):
                c.execute("INSERT INTO rating_stats(target_type,target,n,total) VALUES(%s,%s,%s,%s) "
                          "ON CONFLICT (target_type,target) DO UPDATE SET n = rating_stats.n + %s, total = rating_stats.total + %s",
                          (typ, target, delta_n, delta_total, delta_n, delta_total))
        elif t == "scheduler.month-ended":
            c.execute("""INSERT INTO monthly_reports(month,user_id,total_pages,by_genre)
                         SELECT month, user_id, sum(pages), jsonb_object_agg(genre, pages) FROM monthly_pages
                         WHERE month=%s GROUP BY month, user_id ON CONFLICT DO NOTHING""", (d["month"],))


SAGA_FAIL_EVERY = int(os.getenv("SAGA_FAIL_EVERY", "0"))
_seen_sagas, _failing = set(), set()


def inbox_register(c, event) -> bool:
    """True — повідомлення нове; False — дублікат (вже оброблялось). Робиться в тій самій транзакції, що й обробка."""
    d = event["data"]
    row = c.execute("""INSERT INTO inbox(message_id,event_type,saga_id,payload) VALUES(%s,%s,%s,%s)
                       ON CONFLICT (message_id) DO UPDATE SET duplicates = inbox.duplicates + 1
                       RETURNING (xmax = 0) AS inserted""",
                    (event["event_id"], event["type"], d.get("saga_id"), Jsonb(d))).fetchone()
    return row["inserted"]


def process_saga(event) -> bool:
    """Повертає True, якщо треба відповісти координатору (команда виконана або це дублікат команди)."""
    t, d = event["type"], event["data"]
    if t == "saga.process-completion":
        sid = d["saga_id"]
        if sid not in _seen_sagas:                          # імітація збою: кожна N-та сага падає
            _seen_sagas.add(sid)
            if SAGA_FAIL_EVERY and len(_seen_sagas) % SAGA_FAIL_EVERY == 0:
                _failing.add(sid)
        if sid in _failing:
            raise RuntimeError("Simulated processing error")
        with db.transaction() as c:
            if not inbox_register(c, event):
                infra.log.info("[inbox] duplicate message %s detected", event["event_id"])
                return True                                  # ack міг загубитись — відповідаємо ще раз
            c.execute("INSERT INTO genre_completions(saga_id,user_id,book_id,genre,pages_total,month) VALUES(%s,%s,%s,%s,%s,%s) "
                      "ON CONFLICT DO NOTHING", (sid, d["user_id"], d["book_id"], d["genre"], d["pages_total"], event["occurred_at"][:7]))
        return True
    if t == "saga.compensate":                               # компенсація кроку Analytics
        with db.transaction() as c:
            if not inbox_register(c, event):
                return False
            c.execute("DELETE FROM genre_completions WHERE saga_id=%s", (d["saga_id"],))
            infra.log.info("[saga %s] COMPENSATION in analytics: completion removed", d["saga_id"])
    return False


async def handle_saga(event):
    if await asyncio.to_thread(process_saga, event):
        await broker.publish("analytics.completion-processed", {"saga_id": event["data"]["saga_id"]})


@asynccontextmanager
async def lifespan(app):
    db.init("analytics_db", SCHEMA)
    await broker.connect()
    await broker.consume("analytics.q", ["library.*", "rating.*", "scheduler.*"], handle)
    # черга SAGA-команд з DLQ: 3 спроби обробки, TTL 60 c; після цього повідомлення йде в reading.dlx -> saga.dlq
    await broker.consume("analytics.saga.q", ["saga.process-completion", "saga.compensate"], handle_saga,
                         dlq_key="saga.failed", ttl_ms=60000, retries=3)
    await infra.register_in_consul()
    yield
    await infra.deregister_from_consul()
    await broker.close()


app = FastAPI(title="Analytics Service", lifespan=lifespan)


@app.middleware("http")
async def add_instance_header(request: Request, call_next):
    resp = await call_next(request)
    resp.headers["X-Instance"] = infra.INSTANCE
    return resp


@app.get("/health")
def health():
    return {"status": "ok", "instance": infra.INSTANCE}


@app.get("/dashboard")
def dashboard(x_user_id: int = Header()):
    pages = db.run("SELECT genre, sum(pages)::int AS pages FROM monthly_pages WHERE user_id=%s GROUP BY genre ORDER BY genre", (x_user_id,), "all")
    hist = db.run("SELECT action, count(*)::int AS n FROM history WHERE user_id=%s GROUP BY action", (x_user_id,), "all")
    ratings = db.run("SELECT target_type, target, n, round(total::numeric/NULLIF(n,0),2)::float AS avg FROM rating_stats ORDER BY target_type, target", fetch="all")
    done = db.run("SELECT genre, count(*)::int AS books FROM genre_completions WHERE user_id=%s GROUP BY genre ORDER BY genre", (x_user_id,), "all")
    return {"pages_by_genre": pages, "completed_books_by_genre": done, "history_summary": {h["action"]: h["n"] for h in hist}, "rating_stats": ratings,
            "instance": infra.INSTANCE}


@app.get("/history")
def history(x_user_id: int = Header()):
    return db.run("SELECT action, book_id, title, at FROM history WHERE user_id=%s ORDER BY id", (x_user_id,), "all")


@app.get("/reports")
def reports(x_user_id: int = Header()):
    return db.run("SELECT month, total_pages, by_genre FROM monthly_reports WHERE user_id=%s ORDER BY month", (x_user_id,), "all")
