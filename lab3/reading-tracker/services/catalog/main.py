"""Catalog Service (Catalog & Metadata): пошук книг. Зовнішній Google Books імітується заглушкою,
а Anti-Corruption Layer перетворює його категорії на жанри системи."""
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field

from common import db, infra

SCHEMA = """CREATE TABLE IF NOT EXISTS books(
  id SERIAL PRIMARY KEY, title TEXT NOT NULL, author TEXT NOT NULL, genre TEXT NOT NULL,
  pages INT NOT NULL CHECK (pages > 0), source TEXT NOT NULL, external_id TEXT,
  UNIQUE (source, external_id))"""

GENRES = ["Technical", "Fiction", "History", "Science", "Other"]

# ---- Anti-Corruption Layer: категорія Google Books -> жанр нашої системи ----
CATEGORY_MAPPING = {"Computers": "Technical", "Technology & Engineering": "Technical",
                    "Fiction": "Fiction", "Fiction / Science Fiction": "Fiction",
                    "History": "History", "Science": "Science"}

# ---- заглушка зовнішнього Google Books API (формат відповіді як у справжнього) ----
GOOGLE_BOOKS_STUB = [
    {"id": "gb-101", "volumeInfo": {"title": "Clean Code", "authors": ["Robert C. Martin"], "pageCount": 464, "categories": ["Computers"]}},
    {"id": "gb-102", "volumeInfo": {"title": "Designing Data-Intensive Applications", "authors": ["Martin Kleppmann"], "pageCount": 616, "categories": ["Computers"]}},
    {"id": "gb-103", "volumeInfo": {"title": "Dune", "authors": ["Frank Herbert"], "pageCount": 612, "categories": ["Fiction / Science Fiction"]}},
    {"id": "gb-104", "volumeInfo": {"title": "Sapiens", "authors": ["Yuval Noah Harari"], "pageCount": 512, "categories": ["History"]}},
    {"id": "gb-105", "volumeInfo": {"title": "A Brief History of Time", "authors": ["Stephen Hawking"], "pageCount": 256, "categories": ["Science"]}},
    {"id": "gb-106", "volumeInfo": {"title": "The Pragmatic Programmer", "authors": ["Andrew Hunt"], "pageCount": 352, "categories": ["Computers"]}},
    {"id": "gb-107", "volumeInfo": {"title": "Cooking for Programmers", "authors": ["Some Chef"], "pageCount": 120, "categories": ["Cooking"]}},
]
SEED = [("The Hobbit", "J.R.R. Tolkien", "Fiction", 310), ("Python Crash Course", "Eric Matthes", "Technical", 544),
        ("Guns, Germs, and Steel", "Jared Diamond", "History", 528)]


def google_search(q: str):
    q = q.lower()
    return [b for b in GOOGLE_BOOKS_STUB if q in b["volumeInfo"]["title"].lower()
            or any(q in a.lower() for a in b["volumeInfo"]["authors"])]


def acl_translate(g: dict) -> dict:
    """Чужа модель -> наша модель. Невідома категорія => жанр 'Other'."""
    v = g["volumeInfo"]
    cat = (v.get("categories") or [""])[0]
    return {"title": v["title"], "author": ", ".join(v.get("authors", ["Unknown"])),
            "genre": CATEGORY_MAPPING.get(cat, "Other"), "pages": v.get("pageCount", 1),
            "source": "google", "external_id": g["id"]}


@asynccontextmanager
async def lifespan(app):
    db.init("catalog_db", SCHEMA)
    for i, (t, a, g, p) in enumerate(SEED, 1):
        db.run("INSERT INTO books(title,author,genre,pages,source,external_id) VALUES(%s,%s,%s,%s,'seed',%s) "
               "ON CONFLICT DO NOTHING", (t, a, g, p, f"seed-{i}"))
    await infra.register_in_consul()
    yield
    await infra.deregister_from_consul()


app = FastAPI(title="Catalog Service", lifespan=lifespan)


@app.middleware("http")
async def add_instance_header(request: Request, call_next):
    resp = await call_next(request)
    resp.headers["X-Instance"] = infra.INSTANCE
    return resp


class NewBook(BaseModel):
    title: str
    author: str
    genre: str
    pages: int = Field(gt=0)


@app.get("/health")
def health():
    return {"status": "ok", "instance": infra.INSTANCE}


@app.get("/genres")
def genres():
    return GENRES


@app.get("/books/search")
def search(q: str):
    local = db.run("SELECT * FROM books WHERE title ILIKE %s OR author ILIKE %s ORDER BY id",
                   (f"%{q}%", f"%{q}%"), "all")
    if local:
        return {"source": "local", "instance": infra.INSTANCE, "books": local}
    found = []
    for g in google_search(q):                                   # у каталозі немає -> "Google Books" через ACL
        b = acl_translate(g)
        row = db.run("INSERT INTO books(title,author,genre,pages,source,external_id) VALUES(%s,%s,%s,%s,%s,%s) "
                     "ON CONFLICT (source, external_id) DO UPDATE SET title=EXCLUDED.title RETURNING *",
                     (b["title"], b["author"], b["genre"], b["pages"], b["source"], b["external_id"]), "one")
        found.append(row)
    return {"source": "google (via ACL)", "instance": infra.INSTANCE, "books": found}


@app.get("/books/{book_id}")
def get_book(book_id: int):
    b = db.run("SELECT * FROM books WHERE id=%s", (book_id,), "one")
    if not b:
        raise HTTPException(404, "book not found")
    return {**b, "instance": infra.INSTANCE}


@app.post("/books", status_code=201)
def add_manual(b: NewBook):
    if b.genre not in GENRES:
        raise HTTPException(400, f"genre must be one of {GENRES}")
    return db.run("INSERT INTO books(title,author,genre,pages,source) VALUES(%s,%s,%s,%s,'manual') RETURNING *",
                  (b.title, b.author, b.genre, b.pages), "one")
