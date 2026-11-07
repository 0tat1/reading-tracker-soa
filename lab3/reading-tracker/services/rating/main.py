"""Rating Service (Rating Management). Споживає library.book-completed (щоб оцінювати лише завершені книги),
публікує rating.avg-rating-updated."""
from contextlib import asynccontextmanager

from fastapi import FastAPI, Header, HTTPException, Request
from pydantic import BaseModel, Field

from common import db, infra

broker = infra.Broker()
SCHEMA = """
CREATE TABLE IF NOT EXISTS completed_books(user_id INT, book_id INT, title TEXT, author TEXT, genre TEXT, PRIMARY KEY(user_id, book_id));
CREATE TABLE IF NOT EXISTS ratings(user_id INT, book_id INT, score INT NOT NULL CHECK (score BETWEEN 1 AND 10), comment TEXT, PRIMARY KEY(user_id, book_id));
"""


async def on_book_completed(event):
    d = event["data"]
    await db.arun("INSERT INTO completed_books(user_id,book_id,title,author,genre) VALUES(%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                  (d["user_id"], d["book_id"], d["title"], d["author"], d["genre"]))


@asynccontextmanager
async def lifespan(app):
    db.init("rating_db", SCHEMA)
    await broker.connect()
    await broker.consume("rating.q", ["library.book-completed"], on_book_completed)
    await infra.register_in_consul()
    yield
    await infra.deregister_from_consul()
    await broker.close()


app = FastAPI(title="Rating Service", lifespan=lifespan)


@app.middleware("http")
async def add_instance_header(request: Request, call_next):
    resp = await call_next(request)
    resp.headers["X-Instance"] = infra.INSTANCE
    return resp


class Rate(BaseModel):
    book_id: int
    score: int = Field(ge=1, le=10)
    comment: str | None = None


@app.get("/health")
def health():
    return {"status": "ok", "instance": infra.INSTANCE}


@app.post("/ratings", status_code=201)
async def rate(r: Rate, x_user_id: int = Header()):
    book = await db.arun("SELECT * FROM completed_books WHERE user_id=%s AND book_id=%s", (x_user_id, r.book_id), "one")
    if not book:
        raise HTTPException(409, "only a completed book can be rated")
    prev = await db.arun("SELECT score FROM ratings WHERE user_id=%s AND book_id=%s", (x_user_id, r.book_id), "one")
    await db.arun("INSERT INTO ratings(user_id,book_id,score,comment) VALUES(%s,%s,%s,%s) "
                  "ON CONFLICT (user_id,book_id) DO UPDATE SET score=EXCLUDED.score, comment=EXCLUDED.comment",
                  (x_user_id, r.book_id, r.score, r.comment))
    agg = await db.arun("SELECT round(avg(score),2)::float AS avg, count(*) AS n FROM ratings WHERE book_id=%s", (r.book_id,), "one")
    await broker.publish("rating.avg-rating-updated", {
        "user_id": x_user_id, "book_id": r.book_id, "author": book["author"], "genre": book["genre"],
        "score": r.score, "previous_score": prev["score"] if prev else None,
        "book_avg": agg["avg"], "book_ratings": agg["n"]})
    return {"book_id": r.book_id, "score": r.score, "book_avg": agg["avg"], "instance": infra.INSTANCE}


@app.get("/ratings/{book_id}")
def book_rating(book_id: int):
    agg = db.run("SELECT round(avg(score),2)::float AS avg, count(*) AS n FROM ratings WHERE book_id=%s", (book_id,), "one")
    return {"book_id": book_id, "avg": agg["avg"], "count": agg["n"]}
