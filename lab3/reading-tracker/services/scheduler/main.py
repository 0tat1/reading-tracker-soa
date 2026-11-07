"""Scheduler: періодично публікує scheduler.month-ended (у демо — кожні SCHEDULER_INTERVAL секунд)."""
import asyncio, os
from datetime import datetime, timezone

from common import infra

INTERVAL = int(os.getenv("SCHEDULER_INTERVAL", "120"))


async def main():
    broker = infra.Broker()
    await broker.connect()
    while True:
        await asyncio.sleep(INTERVAL)
        month = datetime.now(timezone.utc).strftime("%Y-%m")
        await broker.publish("scheduler.month-ended", {"month": month})


if __name__ == "__main__":
    asyncio.run(main())
