# Reading Tracker

Мікросервісний бекенд для обліку прочитаних книг і сторінок за жанрами (варіант 22). Користувач реєструється, додає книги зі спільного каталогу на свої полиці, фіксує прочитані сторінки, завершує книги й оцінює їх. Статистика за жанрами, історія та місячні звіти збираються окремим сервісом з подій.

Лабораторна робота 3 додає надійну доставку подій і розподілені транзакції: **Transactional Outbox**, **Transactional Inbox**, **SAGA** з оркестратором, **Dead Letter Queue** та відкат за таймаутом.

Система написана на Python (FastAPI) і запускається локально через Docker Compose.

## Архітектура

```
                 ┌──────────────┐
  client ──────► │ api-gateway  │  :8080 (єдиний публічний порт API)
                 └──────┬───────┘
                        │ REST, пошук через Consul, round-robin
     ┌─────────┬────────┼─────────┬──────────┬───────────┐
     ▼         ▼        ▼         ▼          ▼           ▼
   user    catalog   library    rating   analytics     saga
     │         │        │         │          │           │
     ▼         ▼        ▼         ▼          ▼           ▼
  user_db  catalog_db library_db rating_db analytics_db saga_db   (PostgreSQL)
                        │                    ▲           ▲
                        └──── RabbitMQ ──────┴───────────┘
                         reading.events (topic) + reading.dlx (DLQ)

 Consul: реєстр сервісів і health-check
 RabbitMQ: події між сервісами, команди саги, DLQ
```

| Компонент | Відповідальність |
|---|---|
| `api-gateway` | Єдина точка входу. Перевіряє JWT, знаходить сервіс у Consul, балансує запити між екземплярами (round-robin). |
| `user-service` | Реєстрація, логін, профіль. Видає JWT. |
| `catalog-service` | Каталог книг і жанрів. Відкритий для читання без токена. |
| `library-service` | Полиці користувача, облік сторінок, завершення книги. Записує події через **Outbox**, починає сагу. |
| `rating-service` | Оцінки книг, середній рейтинг. |
| `analytics-service` | Статистика за жанрами, історія, місячні звіти. Приймає повідомлення через **Inbox**, виконує крок саги. |
| `saga-service` | Оркестратор саги «Завершення книги»: журнал саг, таймаути, компенсація. |
| `scheduler` | Публікує подію кінця місяця для місячного звіту (у демо кожні 60 с). |

Кожен сервіс має власну базу даних і не читає чужі дані напряму.

## Маршрутизація та виявлення сервісів

Кожен сервіс при старті реєструється в Consul з HTTP health-check. Gateway для кожного запиту бере з Consul лише здорові екземпляри, тож нові екземпляри підключаються, а несправні відпадають без зміни налаштувань.

| Префікс | Сервіс | Токен потрібен |
|---|---|---|
| `/api/user/register`, `/api/user/login` | user-service | ні |
| `/api/user/me` | user-service | так |
| `/api/catalog/*` | catalog-service | ні |
| `/api/library/*` | library-service | так |
| `/api/rating/*` | rating-service | так |
| `/api/analytics/*` | analytics-service | так |
| `/api/saga/*` | saga-service | так |

Gateway перевіряє токен і передає сервісам ідентифікатор користувача в заголовку `X-User-Id`.

## Надійна доставка подій і SAGA

### Transactional Outbox (library-service)

Зміна даних і подія записуються однією транзакцією бази: подія потрапляє в таблицю `outbox`, а не напряму в RabbitMQ. Фоновий **dispatcher** раз на секунду бере до 20 готових подій (`FOR UPDATE SKIP LOCKED`, тому кілька реплік не беруть одну подію) і публікує їх.

- Статуси події: `new` → `sending` → `sent`, а після 8 невдалих спроб `failed`.
- Пауза між спробами зростає: `спроби × 2` секунд.
- Подія, що зависла в `sending` довше 30 секунд, повертається в роботу.
- Ідентифікатор події (`event_id`) береться з таблиці, тому повторна відправка має той самий id.

### Transactional Inbox (analytics-service)

Перед обробкою повідомлення сервіс записує його `message_id` в таблицю `inbox`. Якщо запис уже є, повідомлення вважається дублікатом: воно не обробляється, а лічильник `duplicates` збільшується. Запис в `inbox` і сама обробка виконуються однією транзакцією. Разом з Outbox це дає доставку «щонайменше раз» і ідемпотентну обробку, тобто фактично «рівно один раз».

### SAGA «Завершення книги» (оркестрація)

Коли користувач дочитує останню сторінку, книга не стає `Completed` одразу. Вона проходить розподілену транзакцію з двох сервісів:

```
Library: книга -> Pending + подія library.saga-started в outbox
   └─► Saga: запис у журнал (started), команда saga.process-completion (sent)
          └─► Analytics: inbox + запис завершення за жанром, відповідь analytics.completion-processed
                 └─► Saga: processed, подія saga.completed
                        └─► Library: Pending -> Completed
```

| Таблиця | Призначення |
|---|---|
| `saga_log` | поточний стан кожної саги: `started` → `sent` → `processed` або `compensated`, причина відкату |
| `saga_steps` | журнал кроків саги з часом |

**Компенсація.** Якщо сага не вдалась, координатор публікує `saga.compensate`: Library повертає книгу з `Pending` на полицю `Reading` (прочитані сторінки лишаються), а Analytics прибирає свій запис. Компенсація ідемпотентна. Після відкату завершення можна повторити командою `POST /api/library/items/{id}/complete`.

**Dead Letter Queue.** Черга команд `analytics.saga.q` має TTL 60 с і налаштований dead-letter exchange. Консьюмер пробує обробити команду 3 рази з паузою в секунду, після чого повідомлення відхиляється й через `reading.dlx` потрапляє в чергу `saga.dlq`. Координатор слухає цю чергу й запускає відкат з причиною `DLQ`.

**Таймаут.** Фоновий таймер координатора раз на 3 секунди шукає саги в стані `started` або `sent`, що тривають довше `SAGA_TIMEOUT` (30 с), і відкочує їх з причиною `TIMEOUT`.

Збій для демонстрації імітується змінною `SAGA_FAIL_EVERY` в analytics-service: кожна N-на сага завжди завершується помилкою.

### Події

Усі події публікуються в обмінник `reading.events` (topic).

| Подія | Хто публікує | Хто слухає |
|---|---|---|
| `user.registered` | user | library |
| `library.book-added`, `library.pages-logged`, `library.book-dropped`, `library.book-removed` | library (через outbox) | analytics |
| `library.saga-started` | library (через outbox) | saga |
| `saga.process-completion` | saga | analytics |
| `analytics.completion-processed` | analytics | saga |
| `saga.completed` | saga | library |
| `saga.compensate` | saga | library, analytics |
| `library.book-completed`, `library.completion-canceled` | library (через outbox) | analytics, rating |
| `rating.avg-rating-updated` | rating | analytics |
| `scheduler.month-ended` | scheduler | analytics |

## Структура проєкту

```
.
├── docker-compose.yml          # усе локальне середовище
├── db/
│   └── init.sql                # створення баз даних для сервісів
├── common/                     # спільний код сервісів
│   ├── db.py                   # PostgreSQL, автостворення бази
│   ├── infra.py                # Consul, RabbitMQ (publish/consume, DLQ, перепідписка), circuit breaker
│   └── requirements.txt
├── services/
│   ├── gateway/                # API Gateway
│   ├── user/                   # реєстрація, логін, JWT
│   ├── catalog/                # каталог книг
│   ├── library/                # полиці, облік сторінок, Outbox, учасник саги
│   ├── rating/                 # оцінки
│   ├── analytics/              # статистика, Inbox, учасник саги
│   ├── saga/                   # координатор саги (saga_log, saga_steps, таймаут)
│   └── scheduler/              # щомісячна подія
├── demo.py                     # наскрізний сценарій (лабораторна 2)
├── demo_lab3.py                # сценарії лабораторної 3
├── report/                     # протокол лабораторної роботи
└── README.md
```

Кожен сервіс у `services/` складається з `main.py` і `Dockerfile`.

## Початок роботи

Потрібно: Docker Desktop (на Windows з WSL 2). Python потрібен лише для демонстраційних скриптів (`pip install requests`).

```bash
docker compose up --build -d
docker compose ps
curl http://localhost:8080/health
```

Перший запуск займає 2–5 хвилин. Бази даних створюються автоматично. Порти, відкриті на хості:

| URL | Що |
|---|---|
| http://localhost:8080 | API Gateway |
| http://localhost:8500 | Consul UI |
| http://localhost:15672 | RabbitMQ UI (логін і пароль `guest` / `guest`) |

Бізнес-сервіси й PostgreSQL доступні тільки всередині мережі Compose.

У RabbitMQ UI на вкладці **Exchanges** є `reading.events` і `reading.dlx`, а на вкладці **Queues and Streams** черги `analytics.saga.q` (з TTL і DLX) та `saga.dlq`.

Зупинити систему: `docker compose down`. З прапорцем `-v` буде видалено й дані баз.

## API

Усі запити йдуть через gateway на `http://localhost:8080`.

### Користувачі

| Метод | Шлях | Опис |
|---|---|---|
| POST | `/api/user/register` | створити користувача (`username`, `password`) |
| POST | `/api/user/login` | отримати `access_token` |
| GET | `/api/user/me` | поточний користувач |

```bash
curl -X POST http://localhost:8080/api/user/register \
  -H 'Content-Type: application/json' \
  -d '{"username":"reader1","password":"secret1"}'

TOKEN=$(curl -s -X POST http://localhost:8080/api/user/login \
  -H 'Content-Type: application/json' \
  -d '{"username":"reader1","password":"secret1"}' | jq -r .access_token)
```

### Каталог

| Метод | Шлях | Опис |
|---|---|---|
| GET | `/api/catalog/genres` | список жанрів |
| GET | `/api/catalog/books/search?q=...` | пошук книг |
| GET | `/api/catalog/books/{id}` | одна книга |
| POST | `/api/catalog/books` | додати книгу (`title`, `author`, `genre`, `pages`) |

### Бібліотека

| Метод | Шлях | Опис |
|---|---|---|
| GET | `/api/library/items` | книги користувача |
| POST | `/api/library/items` | додати книгу на полицю (`book_id`) |
| POST | `/api/library/items/{id}/pages` | записати прочитані сторінки (`pages`); якщо дочитано, стартує сага |
| POST | `/api/library/items/{id}/complete` | завершити книгу (повторити сагу після відкату) |
| POST | `/api/library/items/{id}/drop` | покинути книгу |
| DELETE | `/api/library/items/{id}` | прибрати книгу |
| GET | `/api/library/outbox` | останні події outbox і їх статуси |

```bash
curl -X POST http://localhost:8080/api/library/items \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"book_id": 1}'

curl -X POST http://localhost:8080/api/library/items/1/pages \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"pages": 99999}'
```

Остання команда зарахує всі залишені сторінки й запустить сагу. У відповіді буде `saga_id`.

### Оцінки

| Метод | Шлях | Опис |
|---|---|---|
| POST | `/api/rating/ratings` | оцінити книгу (`book_id`, `score` 1–10, `comment`) |
| GET | `/api/rating/ratings/{book_id}` | оцінки книги |

### Аналітика

| Метод | Шлях | Опис |
|---|---|---|
| GET | `/api/analytics/dashboard` | сторінки за жанрами, завершені книги за жанрами, історія, рейтинги |
| GET | `/api/analytics/history` | історія дій |
| GET | `/api/analytics/reports` | місячні звіти |

### Саги

| Метод | Шлях | Опис |
|---|---|---|
| GET | `/api/saga/sagas` | журнал саг |
| GET | `/api/saga/sagas/{saga_id}` | сага з кроками |

## Демонстрація сценаріїв

```bash
pip install requests
python demo_lab3.py
```

**1. Успішна сага.** Скрипт створює користувача, додає книги й дочитує їх. Для кожної книги видно журнал кроків саги, стан `processed` і полицю `Completed`.

**2. Збій у консьюмері: DLQ і відкат.** Запустити Analytics так, щоб кожна друга сага падала:

```powershell
$env:SAGA_FAIL_EVERY="2"; docker compose up -d analytics-service
python demo_lab3.py
```

Друга сага після трьох невдалих спроб потрапляє в DLQ і відкочується (`compensated`, причина `DLQ`), книга повертається на полицю `Reading`, а скрипт повторює завершення командою `complete`.

**3. Таймаут.** Зупинити Analytics, завершити книги й почекати 30 секунд:

```bash
docker compose stop analytics-service
python demo_lab3.py
```

Саги відкотяться з причиною `TIMEOUT`.

**4. Outbox при недоступному брокері.**

```bash
docker compose stop rabbitmq
```

Додати книгу: API відповість `201`, а подія залишиться в `outbox` зі статусом `new`, лічильник спроб зростатиме. Після `docker compose start rabbitmq` подію буде відправлено (`sent`). Якщо брокер вимкнений довше за 8 спроб, подія отримає `failed`, її повертають у чергу так:

```bash
docker compose exec postgres psql -U postgres -d library_db \
  -c "update outbox set status='new', attempts=0, available_at=now() where status='failed'"
```

Стан таблиць можна переглянути напряму:

```bash
docker compose exec postgres psql -U postgres -d saga_db \
  -c "select left(saga_id,8) as saga, state, reason from saga_log order by started_at desc limit 5"

docker compose exec postgres psql -U postgres -d analytics_db \
  -c "select event_type, status, duplicates from inbox order by received_at desc limit 5"
```

## Масштабування

```bash
docker compose up -d --scale catalog-service=3 --scale library-service=2
docker compose ps
python demo.py
```

У Consul UI у `catalog-service` з'являться три екземпляри, а gateway розподіляє запити між ними. Кілька реплік `library-service` безпечні для outbox: dispatcher захоплює події через `SKIP LOCKED`, тому одну подію не візьмуть двічі.

Повернути назад: `docker compose up -d --scale catalog-service=1 --scale library-service=1`.

## Конфігурація

Налаштування задаються змінними середовища в `docker-compose.yml`.

| Змінна | Значення за замовчуванням | Призначення |
|---|---|---|
| `JWT_SECRET` | `change-me-in-prod` | ключ підпису JWT |
| `SAGA_TIMEOUT` | `30` | секунд до відкату саги за таймаутом (saga-service) |
| `SAGA_FAIL_EVERY` | `0` | кожна N-на сага падає в Analytics; `0` означає без збоїв |
| `OUTBOX_MAX_ATTEMPTS` | `8` | кількість спроб відправки події до статусу `failed` |

Для будь-якого середовища, крім локального, змініть `JWT_SECRET` і паролі.

## Корисні команди

```bash
docker compose logs -f saga-service analytics-service library-service   # логи
docker compose stop catalog-service                                      # імітація збою (circuit breaker у Library)
docker compose down                                                      # зупинити
docker compose down -v                                                   # зупинити і видалити дані баз
```

## Обмеження

- Один координатор саги: це точка ризику, як зазначено в методичці.
- Події зі статусом `failed` в outbox повертаються в чергу вручну.
- Між кроками саги немає ізоляції: проміжний стан `Pending` видно користувачеві.
- Координатор спершу публікує повідомлення, потім оновлює стан. Якщо він упаде між цими діями, повтор виконає таймаут.
