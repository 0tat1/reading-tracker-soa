"""Демонстрація всієї системи через API Gateway.  Запуск:  python demo.py   (потрібен: pip install requests)"""
import collections, os, sys, time, uuid
import requests

GW = os.getenv("GATEWAY", "http://localhost:8080")
H = lambda t=None: {"Authorization": f"Bearer {t}"} if t else {}


def show(title, r):
    print(f"\n=== {title}\n    {r.request.method} {r.request.url}  ->  {r.status_code}  [instance: {r.headers.get('X-Instance')}]")
    try:
        print("    ", r.json())
    except Exception:
        print("    ", r.text)
    return r


print("Сервіси, які бачить Consul:")
for k, v in requests.get(f"{GW}/services").json().items():
    print(f"  {k:20s} {len(v) if isinstance(v, list) else v} інстанс(ів) {v if isinstance(v, list) else ''}")

user = f"reader-{uuid.uuid4().hex[:6]}"
show("1. Реєстрація (sync) + подія user.registered (async)", requests.post(f"{GW}/api/user/register", json={"username": user, "password": "secret1"}))
tok = show("2. Логін -> JWT", requests.post(f"{GW}/api/user/login", json={"username": user, "password": "secret1"})).json()["access_token"]
show("3. Запит без токена (очікуємо 401)", requests.get(f"{GW}/api/library/items"))
books = show("4. Пошук книги (Catalog; Google Books через ACL)", requests.get(f"{GW}/api/catalog/books/search", params={"q": "Clean Code"})).json()["books"]
bid = books[0]["id"]
item = show("5. Додати книгу в бібліотеку (Library -> Catalog синхронно через Consul)", requests.post(f"{GW}/api/library/items", json={"book_id": bid}, headers=H(tok))).json()
show("6. Дубль (очікуємо 409)", requests.post(f"{GW}/api/library/items", json={"book_id": bid}, headers=H(tok)))
iid = item["id"]
show("7. Зафіксувати 200 сторінок (подія library.pages-logged)", requests.post(f"{GW}/api/library/items/{iid}/pages", json={"pages": 200}, headers=H(tok)))
show("8. Оцінити незавершену книгу (очікуємо 409)", requests.post(f"{GW}/api/rating/ratings", json={"book_id": bid, "score": 9}, headers=H(tok)))
show("9. Дочитати книгу (стартує SAGA; книга -> Completed після підтвердження)", requests.post(f"{GW}/api/library/items/{iid}/pages", json={"pages": 9999}, headers=H(tok)))
time.sleep(6)  # Лаба 3: завершення книги тепер йде через SAGA (Pending -> Completed) + eventual consistency
show("10. Оцінити завершену книгу (rating.avg-rating-updated)", requests.post(f"{GW}/api/rating/ratings", json={"book_id": bid, "score": 9, "comment": "great"}, headers=H(tok)))
time.sleep(2)
show("11. Дашборд аналітики (побудований з подій)", requests.get(f"{GW}/api/analytics/dashboard", headers=H(tok)))
show("12. Історія користувача", requests.get(f"{GW}/api/analytics/history", headers=H(tok)))

print("\n=== 13. Балансування: 12 запитів до catalog-service через gateway")
cnt = collections.Counter(requests.get(f"{GW}/api/catalog/books/1").headers.get("X-Instance") for _ in range(12))
for inst, n in cnt.items():
    print(f"    інстанс {inst}: {n} запитів")
print("    (запусти з --scale catalog-service=3, щоб побачити розподіл між трьома інстансами)")
