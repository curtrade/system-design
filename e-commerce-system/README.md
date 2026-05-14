# Дизайн e-commerce системы на микросервисах

Сервисы: **Orders**, **Payments**, **Inventory**, **Notifications**.
Стек: Node.js (Fastify/NestJS), PostgreSQL (по сервису), Kafka, Redis, gRPC.

---

## Допущения

- Нагрузка: ~1000 заказов/мин в пике, ~50k SKU
- Корректность денег критична; oversell допустим в редких случаях с компенсацией
- Пользователь ждёт ответа на «Купить» не более 2–3 сек
- Eventual consistency между сервисами приемлема

---

## 1. Как сервисы общаются

**Гибрид: sync для критичного пути + async для фанаута.**

| Связь | Тип | Протокол | Почему |
|---|---|---|---|
| Orders → Inventory (reserve) | sync | **gRPC** | Нужен немедленный ответ «есть/нет товара» |
| Orders → Payments (charge) | sync | **gRPC** | Пользователь ждёт результат списания |
| Orders → Kafka (`OrderConfirmed`) | async | **Kafka** | Фанаут на Inventory.commit и Notifications |
| Inventory → Kafka (`StockChanged`) | async | Kafka | Аналитика, кэши каталога |
| → Notifications | async | Kafka consumer | Email/push не должны блокировать оформление |

### Паттерн оркестрации

**Orders — это saga-оркестратор.** Он явно вызывает Inventory и Payments, ведёт состояние саги в своей БД. Хореография (чистые события) для 4 сервисов даёт меньше выгод, чем теряет в наблюдаемости.

### Outbox pattern

В каждом сервисе: запись «изменение БД + событие» атомарна (одна транзакция в локальный outbox-таблицу), отдельный publisher шлёт в Kafka. Это убирает класс багов «БД закоммитили, событие потерялось».

### Идемпотентность

Каждый запрос несёт `Idempotency-Key` (для Inventory/Payments — это `orderId` + `attemptId`). Kafka consumers дедуплицируют по `(eventId, consumerGroup)`.

### Happy path

```
Client ──POST /orders──► Orders
                          │
                          ├─ INSERT order(status=PENDING) [tx]
                          ├─ gRPC Inventory.Reserve(orderId, items, ttl=15m) ──► reservation locked
                          ├─ gRPC Payments.Charge(orderId, amount, idemKey)  ──► card charged
                          ├─ UPDATE order(status=CONFIRMED) + outbox(OrderConfirmed) [tx]
                          ▼
                       Kafka topic: orders.events
                          ├──► Inventory consumer: Commit(reservationId) → стоки списываются
                          └──► Notifications consumer: send email/push
```

---

## 2. Сценарий: Payments упал после резерва товара

**Защита в три слоя — от лёгкого к тяжёлому:**

1. **TTL у резерва.** `Inventory.Reserve` создаёт резерв с истечением (15 мин). Если ничего не подтверждено — сам отпустится. Safety net.

2. **Явная компенсация саги.** Orders видит ошибку от Payments → вызывает `Inventory.Release(reservationId)`. Помечает заказ `FAILED_PAYMENT`. Это happy path для отказа.

3. **Outbox-событие `OrderFailed`.** Если шаг 2 не дозвонился до Inventory (сетевой сбой, рестарт пода), событие лежит в outbox → publisher отправит в Kafka → Inventory consumer обработает идемпотентно по `reservationId`.

### Что если Orders сам упал в середине?

При рестарте — *saga recovery worker* читает заказы в статусе `PENDING` старше N секунд и доводит до терминального состояния:
- либо retry charge (если ещё в окне идемпотентности),
- либо release + `FAILED`.

---

## 3. Сценарий: деньги списались, но Inventory не подтвердил commit

Это болезненный кейс — у системы возник **долг перед клиентом**, который надо закрыть одним из двух способов: **доставить товар** или **вернуть деньги**.

### Алгоритм

```
Payments OK → Orders publishes OrderConfirmed (через outbox, гарантированно)
                  │
                  ▼
Inventory consumer: Commit(reservationId)
   ├─ success → OrderFulfilled event
   └─ fail   → retry с экспоненциальным backoff (3–5 попыток)
        └─ всё ещё fail → DLQ + alert
              └─ Saga compensator: Payments.Refund(orderId) + Order.status=REFUNDED
                    └─ Notifications: «не смогли выполнить, деньги возвращены»
```

### Ключевые свойства

- `OrderConfirmed` не теряется — outbox + Kafka retention. Даже если Inventory лежит час, событие дождётся.
- `Commit(reservationId)` идемпотентен. Повторный вызов на уже закоммиченный резерв — no-op.
- Резерв уже сделан (шаг 1), так что в 99% случаев товар на складе физически есть — commit это просто перевод из reserved в sold. Реальный сбой = инфраструктурный, не «товар кончился».
- Если retry-петля исчерпана → **автоматический refund**. Никаких «деньги списались, заказ потерялся» — система всегда сходится в одно из двух состояний.
- Ручная очередь (admin UI) — последний рубеж для случаев, когда refund тоже не идёт (закрытая карта и т.п.).

---

## 4. Trade-offs

| Решение | Плюс | Минус | Когда пересмотреть |
|---|---|---|---|
| **Оркестрация (Orders как saga)** | Понятный workflow, легко дебажить, одно место для бизнес-логики | Orders «знает» про всех; bottleneck для эволюции | Когда воркфлоу разветвится на 8+ шагов или появятся независимые домены |
| **Sync для Payments** | Юзер сразу видит результат | Хвостовая латентность Payments = латентность UX; каскадные сбои | Перейти на «оплата в фоне» когда конверсия упрётся в latency |
| **Kafka, а не RabbitMQ/NATS** | Replay, аудит, durability, аналитика бесплатно | Операционная сложность (ZK/KRaft, партиции, ребалансы); overkill на малом масштабе | На старте — NATS JetStream проще; Kafka когда нужен replay для аналитики |
| **DB per service + outbox** | Чёткие границы, независимые деплои, нет распределённых транзакций | Дубль данных (e.g. snapshot товаров в заказе), eventual consistency в UI | Снапшоты — это фича, не баг (исторические цены) |
| **TTL-резерв 15 мин** | Самовосстановление при потере оркестратора | Возможен oversell, если флоу длиннее TTL; нужна синхронизация часов | Снизить TTL при росте конкуренции за сток; добавить early-release при отказе |
| **Auto-refund после N retry** | Система всегда сходится; нет «зависших» денег | Клиент может получить refund вместо товара, который физически есть на складе | Добавить human-in-the-loop для high-value заказов (> X $) |
| **gRPC между сервисами** | Строгие контракты (.proto), производительность, streaming | Сложнее тулинг (curl/Postman), бинарный формат в логах | Если команда страдает с tooling — REST/JSON с OpenAPI |

---

## Что бы пересмотрел при росте

- **Geo/multi-region**: Payments провайдер часто региональный → роутинг саг по региону пользователя.
- **Read-models**: для «мои заказы» — отдельная проекция через CDC, чтобы не нагружать Orders OLTP.
- **Идемпотентность Payments на уровне провайдера**: Stripe/Adyen имеют свой `Idempotency-Key` — обязательно использовать, не полагаться только на свой слой.
- **Inventory: hot SKU и distributed counter**: один популярный товар = горячая строка. Sharding по SKU или Redis-cluster со скриптами Lua для атомарного резерва.
- **Observability с самого начала**: trace-id сквозь Kafka headers, иначе сагу через 4 сервиса не отладишь.
