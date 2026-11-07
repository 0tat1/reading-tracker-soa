"""Лаба 3: Outbox + Inbox + SAGA + DLQ. Запуск:  python demo_lab3.py   (pip install requests)
Для сценарію збою запусти Analytics з  SAGA_FAIL_EVERY=2  (кожна 2-га сага падає) — див. README."""
import os, time, uuid
import requests

GW = os.getenv("GATEWAY", "http://localhost:8080")
H = lambda t: {"Authorization": f"Bearer {t}"}


def show(title, r):
    print(f"\n=== {title}\n    {r.request.method} {r.request.url}  ->  {r.status_code}")
    try:
        print("    ", r.json())
    except Exception:
        print("    ", r.text)
    return r


def wait_saga(tok, saga_id, timeout=90):
    """Чекає, поки SAGA вийде зі станів started/sent."""
    end = time.time() + timeout
    while time.time() < end:
        s = requests.get(f"{GW}/api/saga/sagas/{saga_id}", headers=H(tok)).json()
        if s.get("state") in ("processed", "compensated"):
            return s
        time.sleep(1)
    return s


user = f"reader-{uuid.uuid4().hex[:6]}"
requests.post(f"{GW}/api/user/register", json={"username": user, "password": "secret1"})
tok = requests.post(f"{GW}/api/user/login", json={"username": user, "password": "secret1"}).json()["access_token"]
books = requests.get(f"{GW}/api/catalog/books/search", params={"q": "a"}).json()["books"][:3]
print(f"Користувач {user}, книг для тесту: {len(books)}")

results = []
for i, b in enumerate(books, 1):
    item = show(f"{i}.1 Додати книгу «{b['title']}» (запис у library + outbox в одній транзакції)",
                requests.post(f"{GW}/api/library/items", json={"book_id": b["id"]}, headers=H(tok))).json()
    r = show(f"{i}.2 Дочитати книгу -> shelf=Pending, стартує SAGA",
             requests.post(f"{GW}/api/library/items/{item['id']}/pages", json={"pages": 99999}, headers=H(tok))).json()
    s = wait_saga(tok, r["saga_id"])
    print(f"    >>> SAGA {s['saga_id'][:8]}…  стан: {s['state']}  причина: {s.get('reason')}")
    for st in requests.get(f"{GW}/api/saga/sagas/{r['saga_id']}", headers=H(tok)).json()["steps"]:
        print(f"        - {st['step']}")
    results.append((item["id"], s["state"]))

print("\n=== Підсумок: стан полиць у Library")
for it in requests.get(f"{GW}/api/library/items", headers=H(tok)).json():
    print(f"    {it['title'][:30]:30s} shelf={it['shelf']:10s} pages={it['pages_read']}/{it['pages_total']}")

print("\n=== Outbox (останні події)")
for o in requests.get(f"{GW}/api/library/outbox", params={"limit": 8}, headers=H(tok)).json():
    print(f"    #{o['id']:<4} {o['event_type']:28s} {o['status']:8s} спроб: {o['attempts']}")

comp = [i for i, st in results if st == "compensated"]
if comp:
    iid = comp[0]
    show(f"Повторити завершення відкоченої книги (item {iid}) — нова SAGA", requests.post(f"{GW}/api/library/items/{iid}/complete", headers=H(tok)))
