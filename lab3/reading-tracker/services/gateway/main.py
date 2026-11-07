"""API Gateway: єдина точка входу. Перевіряє JWT, знаходить сервіс у Consul, балансує (round-robin) і проксіює запит."""
import itertools, os
from contextlib import asynccontextmanager

import httpx, jwt
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse

from common import infra

JWT_SECRET = os.getenv("JWT_SECRET", "dev-secret")
PUBLIC = {("user", "register"), ("user", "login")}          # не потребують токена
PUBLIC_SERVICES = {"catalog"}                                # каталог відкритий
ROUTES = {"user": "user-service", "catalog": "catalog-service", "library": "library-service",
          "rating": "rating-service", "analytics": "analytics-service", "saga": "saga-service"}
_rr = itertools.count()


@asynccontextmanager
async def lifespan(app):
    await infra.register_in_consul()
    yield
    await infra.deregister_from_consul()


app = FastAPI(title="Reading Tracker - API Gateway", lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "ok", "instance": infra.INSTANCE}


@app.get("/services")
async def services():
    """Показує, які інстанси кожного сервісу зараз бачить Consul (для демонстрації discovery/масштабування)."""
    out = {}
    for short, name in ROUTES.items():
        try:
            out[name] = await infra.discover(name)
        except Exception as e:
            out[name] = f"error: {e}"
    return out


@app.api_route("/api/{service}/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
async def proxy(service: str, path: str, request: Request):
    if service not in ROUTES:
        return JSONResponse({"detail": f"unknown service '{service}'"}, status_code=404)

    headers = {k: v for k, v in request.headers.items() if k.lower() not in ("host", "content-length", "x-user-id")}
    if service not in PUBLIC_SERVICES and (service, path) not in PUBLIC:
        auth = request.headers.get("authorization", "")
        if not auth.startswith("Bearer "):
            return JSONResponse({"detail": "missing bearer token"}, status_code=401)
        try:
            claims = jwt.decode(auth[7:], JWT_SECRET, algorithms=["HS256"])
        except jwt.PyJWTError:
            return JSONResponse({"detail": "invalid or expired token"}, status_code=401)
        headers["x-user-id"] = str(claims["sub"])             # сервіси довіряють gateway

    try:
        urls = await infra.discover(ROUTES[service])         # 1. Consul: список здорових інстансів
    except Exception as e:
        return JSONResponse({"detail": f"discovery failed: {e}"}, status_code=503)
    if not urls:
        return JSONResponse({"detail": f"no healthy instances of {ROUTES[service]}"}, status_code=503)

    body = await request.body()
    start = next(_rr)
    for i in range(len(urls)):                                # 2. round-robin + failover
        url = urls[(start + i) % len(urls)]
        try:
            async with httpx.AsyncClient(timeout=10) as c:
                r = await c.request(request.method, f"{url}/{path}", params=request.query_params,
                                    content=body, headers=headers)
            out = Response(r.content, status_code=r.status_code, media_type=r.headers.get("content-type"))
            out.headers["X-Upstream"] = url
            if "x-instance" in r.headers:
                out.headers["X-Instance"] = r.headers["x-instance"]
            return out
        except httpx.TransportError:
            continue
    return JSONResponse({"detail": "all instances failed"}, status_code=502)
