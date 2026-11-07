"""Спільна інфраструктурна логіка: Consul (discovery), RabbitMQ (події), circuit breaker."""
import asyncio, itertools, json, logging, os, socket, time, uuid
from datetime import datetime, timezone

import aio_pika
import httpx

log = logging.getLogger("infra")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

SERVICE_NAME = os.getenv("SERVICE_NAME", "service")
SERVICE_PORT = int(os.getenv("SERVICE_PORT", "8000"))
CONSUL_URL = os.getenv("CONSUL_URL", "http://consul:8500")
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq/")
INSTANCE = os.getenv("HOSTNAME") or socket.gethostname()   # у Docker це id контейнера
EXCHANGE = "reading.events"
DLX = "reading.dlx"          # dead-letter exchange: сюди RabbitMQ переносить "мертві" повідомлення


def my_address() -> str:
    return os.getenv("ADVERTISE_ADDRESS") or socket.gethostbyname(socket.gethostname())


# ----------------------------------------------------------------- Consul
async def register_in_consul():
    """Реєструє цей інстанс у Consul. ID містить hostname, тому кілька копій не конфліктують."""
    addr = my_address()
    payload = {
        "Name": SERVICE_NAME,
        "ID": f"{SERVICE_NAME}-{INSTANCE}",
        "Address": addr,
        "Port": SERVICE_PORT,
        "Check": {
            "HTTP": f"http://{addr}:{SERVICE_PORT}/health",
            "Interval": "5s",
            "Timeout": "2s",
            "DeregisterCriticalServiceAfter": "30s",
        },
    }
    async with httpx.AsyncClient(timeout=3) as c:
        for attempt in range(1, 31):
            try:
                r = await c.put(f"{CONSUL_URL}/v1/agent/service/register", json=payload)
                if r.status_code == 200:
                    log.info("registered in Consul as %s (%s:%s)", payload["ID"], addr, SERVICE_PORT)
                    return
            except Exception as e:
                log.warning("Consul not ready (%s/30): %s", attempt, e)
            await asyncio.sleep(2)
    raise RuntimeError("could not register in Consul")


async def deregister_from_consul():
    try:
        async with httpx.AsyncClient(timeout=3) as c:
            await c.put(f"{CONSUL_URL}/v1/agent/service/deregister/{SERVICE_NAME}-{INSTANCE}")
    except Exception:
        pass


_rr = itertools.count()


async def discover(name: str) -> list[str]:
    """Повертає base-URL усіх *здорових* інстансів сервісу (із Consul)."""
    async with httpx.AsyncClient(timeout=3) as c:
        r = await c.get(f"{CONSUL_URL}/v1/health/service/{name}", params={"passing": "true"})
        r.raise_for_status()
    return [f"http://{e['Service']['Address']}:{e['Service']['Port']}" for e in r.json()]


class CircuitBreaker:
    """CLOSED -> (N помилок) -> OPEN -> (через reset_timeout) -> HALF_OPEN -> успіх -> CLOSED."""

    def __init__(self, failures=3, reset_timeout=10.0):
        self.max_failures, self.reset_timeout = failures, reset_timeout
        self.failures, self.opened_at = 0, None

    @property
    def state(self):
        if self.opened_at is None:
            return "CLOSED"
        return "HALF_OPEN" if time.monotonic() - self.opened_at >= self.reset_timeout else "OPEN"

    def allow(self):
        return self.state != "OPEN"

    def success(self):
        self.failures, self.opened_at = 0, None

    def failure(self):
        self.failures += 1
        if self.failures >= self.max_failures or self.state == "HALF_OPEN":
            self.opened_at = time.monotonic()


class ServiceUnavailable(Exception):
    pass


async def call_service(breaker: CircuitBreaker, name: str, method: str, path: str, **kw) -> httpx.Response:
    """Синхронний виклик іншого сервісу: discovery у Consul + round-robin + failover + circuit breaker."""
    if not breaker.allow():
        raise ServiceUnavailable(f"circuit breaker OPEN for {name}")
    try:
        urls = await discover(name)
        if not urls:
            raise ServiceUnavailable(f"no healthy instances of {name}")
        start = next(_rr)
        last = None
        for i in range(len(urls)):                      # якщо інстанс не відповів — пробуємо наступний
            url = urls[(start + i) % len(urls)]
            try:
                async with httpx.AsyncClient(timeout=5) as c:
                    r = await c.request(method, url + path, **kw)
                breaker.success()
                return r
            except httpx.TransportError as e:
                last = e
        raise ServiceUnavailable(f"all instances of {name} failed: {last}")
    except ServiceUnavailable:
        breaker.failure()
        raise
    except Exception as e:
        breaker.failure()
        raise ServiceUnavailable(str(e))


# --------------------------------------------------------------- RabbitMQ
class Broker:
    """Topic exchange 'reading.events'. Routing key = назва події (наприклад library.pages-logged).
    Має «наглядача»: якщо після розриву зв'язку з RabbitMQ канал не відновився сам — перевідкриває його
    і заново підписує всі черги (споживачі не «зникають» після рестарту брокера)."""

    def __init__(self):
        self.conn = self.channel = self.exchange = self.dlx = None
        self._subs: list[tuple] = []          # усі підписки: (queue_name, keys, handler, kwargs)
        self._watch_task = None
        self._reconnected = False

    async def _open_channel(self):
        self.channel = await self.conn.channel()
        await self.channel.set_qos(prefetch_count=10)
        self.exchange = await self.channel.declare_exchange(EXCHANGE, aio_pika.ExchangeType.TOPIC, durable=True)
        self.dlx = await self.channel.declare_exchange(DLX, aio_pika.ExchangeType.DIRECT, durable=True)

    async def connect(self):
        for attempt in range(1, 61):
            try:
                self.conn = await aio_pika.connect_robust(RABBITMQ_URL)
                await self._open_channel()
                log.info("connected to RabbitMQ")
                self.conn.reconnect_callbacks.add(lambda *a: setattr(self, "_reconnected", True))
                self._watch_task = asyncio.create_task(self._watchdog())
                return
            except Exception as e:
                log.warning("RabbitMQ not ready (%s/60): %s", attempt, e)
                await asyncio.sleep(2)
        raise RuntimeError("could not connect to RabbitMQ")

    async def _watchdog(self):
        """Після reconnect aio-pika інколи не відновлює споживачів -> підписуємося наново на новому каналі."""
        while True:
            await asyncio.sleep(3)
            if not self._reconnected:
                continue
            try:
                log.warning("RabbitMQ reconnected - re-subscribing %s queue(s)", len(self._subs))
                try:
                    await self.channel.close()
                except Exception:
                    pass
                await self._open_channel()
                for name, keys, handler, kw in self._subs:
                    await self._subscribe(name, keys, handler, **kw)
                self._reconnected = False
            except Exception as e:
                log.warning("RabbitMQ re-subscribe failed, will retry: %s", e)

    async def publish(self, key: str, data: dict, event_id: str | None = None):
        event = {"event_id": event_id or str(uuid.uuid4()), "type": key,
                 "occurred_at": datetime.now(timezone.utc).isoformat(), "data": data}
        await asyncio.wait_for(self.exchange.publish(
            aio_pika.Message(json.dumps(event).encode(), content_type="application/json",
                             delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
            routing_key=key), timeout=5)
        log.info("PUBLISHED %s %s", key, data)

    async def consume(self, queue_name: str, keys: list[str], handler, **kw):
        self._subs.append((queue_name, keys, handler, kw))
        await self._subscribe(queue_name, keys, handler, **kw)

    async def _subscribe(self, queue_name: str, keys: list[str], handler, *, exchange: str | None = None,
                         dlq_key: str | None = None, ttl_ms: int | None = None, retries: int = 1):
        """Створює durable-чергу, підписує її на routing keys і викликає handler(event) для кожного повідомлення.
        Усі репліки сервісу читають одну чергу (competing consumers).
        dlq_key/ttl_ms: якщо задано — повідомлення, яке не оброблено (reject після `retries` спроб) або прострочене (TTL),
        RabbitMQ автоматично переносить у dead-letter exchange з цим routing key (Dead Letter Queue)."""
        args = {}
        if dlq_key:
            args["x-dead-letter-exchange"] = DLX
            args["x-dead-letter-routing-key"] = dlq_key
        if ttl_ms:
            args["x-message-ttl"] = ttl_ms
        queue = await self.channel.declare_queue(queue_name, durable=True, arguments=args or None)
        for k in keys:
            await queue.bind(self.dlx if exchange == "dlx" else self.exchange, routing_key=k)

        async def on_message(msg: aio_pika.IncomingMessage):
            async with msg.process(requeue=False):          # ack при успіху; при помилці — reject (-> DLX, якщо налаштовано)
                event = json.loads(msg.body)
                log.info("CONSUMED %s by %s", event["type"], INSTANCE)
                for attempt in range(1, retries + 1):
                    try:
                        await handler(event)
                        return
                    except Exception as e:
                        log.warning("handler failed %s (attempt %s/%s): %s", event["type"], attempt, retries, e)
                        if attempt == retries:
                            raise
                        await asyncio.sleep(1)

        await queue.consume(on_message)

    async def close(self):
        if self._watch_task:
            self._watch_task.cancel()
        if self.conn:
            await self.conn.close()
