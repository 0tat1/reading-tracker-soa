"""User Service (контекст User Management): реєстрація, логін, видача JWT. Публікує user.registered."""
import hashlib, hmac, os, time
from contextlib import asynccontextmanager

import jwt
from fastapi import FastAPI, Header, HTTPException, Request
from pydantic import BaseModel, Field

from common import db, infra

JWT_SECRET = os.getenv("JWT_SECRET", "dev-secret")
broker = infra.Broker()
SCHEMA = """CREATE TABLE IF NOT EXISTS users(
  id SERIAL PRIMARY KEY, username TEXT UNIQUE NOT NULL, password_hash TEXT NOT NULL, salt TEXT NOT NULL)"""


def hash_pw(password: str, salt: str) -> str:
    return hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 100_000).hex()


@asynccontextmanager
async def lifespan(app):
    db.init("user_db", SCHEMA)
    await broker.connect()
    await infra.register_in_consul()
    yield
    await infra.deregister_from_consul()
    await broker.close()


app = FastAPI(title="User Service", lifespan=lifespan)


@app.middleware("http")
async def add_instance_header(request: Request, call_next):
    resp = await call_next(request)
    resp.headers["X-Instance"] = infra.INSTANCE
    return resp


class Credentials(BaseModel):
    username: str = Field(min_length=3)
    password: str = Field(min_length=4)


@app.get("/health")
def health():
    return {"status": "ok", "instance": infra.INSTANCE}


@app.post("/register", status_code=201)
async def register(c: Credentials):
    salt = os.urandom(8).hex()
    try:
        row = await db.arun("INSERT INTO users(username,password_hash,salt) VALUES(%s,%s,%s) RETURNING id",
                            (c.username, hash_pw(c.password, salt), salt), "one")
    except Exception:
        raise HTTPException(409, "username already exists")
    await broker.publish("user.registered", {"user_id": row["id"], "username": c.username})   # async-подія
    return {"id": row["id"], "username": c.username, "instance": infra.INSTANCE}


@app.post("/login")
def login(c: Credentials):
    u = db.run("SELECT * FROM users WHERE username=%s", (c.username,), "one")
    if not u or not hmac.compare_digest(u["password_hash"], hash_pw(c.password, u["salt"])):
        raise HTTPException(401, "wrong username or password")
    token = jwt.encode({"sub": str(u["id"]), "username": u["username"], "exp": int(time.time()) + 3600},
                       JWT_SECRET, algorithm="HS256")
    return {"access_token": token, "token_type": "bearer", "instance": infra.INSTANCE}


@app.get("/me")
def me(x_user_id: int = Header()):
    u = db.run("SELECT id, username FROM users WHERE id=%s", (x_user_id,), "one")
    if not u:
        raise HTTPException(404, "user not found")
    return u
