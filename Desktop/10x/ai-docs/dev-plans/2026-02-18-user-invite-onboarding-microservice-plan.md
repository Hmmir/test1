# План: микросервис приглашения пользователей и готовность продукта

Дата: 2026-02-18  
Проект: `C:\Users\alien\Desktop\10x`

## 1) Что подтверждено по текущему проекту (факты)

- Бэкенд сейчас монолитный совместимый слой на Python `http.server` + SQLite: `backend_compat/server.py`, `backend_compat/storage.py`.
- Для подключения клиента уже есть ручные админ-роуты:
  - `POST /api/admin/spreadsheets/register`
  - `POST /api/admin/wb/tokens/add`
  - `POST /api/ss/wb/token/get`
  (см. `backend_compat/README.md`, `backend_compat/server.py`).
- Авторизация в текущем бэкенде только через общий `X-Api-Key` (`BTLZ_API_KEY`), без ролей/пользователей (`backend_compat/server.py`).
- В хранилище есть `spreadsheets.owner_email`, но нет жизненного цикла приглашений, активации пользователя и аудита действий (`backend_compat/storage.py`).
- В библиотеке `libs/BtlzApi/Code.js` уже зашит контракт admin API уровня пользователей (`/admin/user/...`, `activation_link`), который в `backend_compat/server.py` пока не реализован.
- В документации прямо отмечено, что для multi-client нужно добавить auth, rate limiting и audit logs (`backend_compat/README.md`).

## 2) Цель плана

Построить отдельный onboarding-микросервис, который:
- приглашает новых пользователей (invite -> activation -> ready),
- автоматически подключает их к текущему `backend_compat`,
- добавляет минимально необходимую эксплуатационную готовность (безопасность, наблюдаемость, управляемость) для работы с новыми клиентами.

## 3) Минимально-инвазивная целевая архитектура

1. Новый сервис: `onboarding-service` (отдельный процесс и БД).
2. `backend_compat` остается источником бизнес-данных и текущих dataset endpoints.
3. `onboarding-service` оркестрирует подключение клиента через существующие вызовы:
   - register spreadsheet,
   - save WB token,
   - verify token mapping,
   - smoke-check базовых dataset/update операций.
4. API для админ-панели/оператора концентрируется в `onboarding-service` и постепенно может закрыть контракт из `libs/BtlzApi/Code.js`.

## 4) Пошаговый план реализации

### Шаг 1. Зафиксировать продуктовый флоу onboarding как state machine

Статусы (минимум):
- `invited` -> `email_confirmed` -> `workspace_registered` -> `token_added` -> `validated` -> `active`
- ответвления: `failed`, `expired`, `revoked`

Выход шага:
- единая схема переходов + причины ошибок (чтобы не было "серых" состояний).

### Шаг 2. Спроектировать контракт onboarding API

Минимальные endpoint'ы:
- `POST /onboarding/invites` (создать приглашение)
- `POST /onboarding/invites/{token}/accept` (принять приглашение)
- `POST /onboarding/workspaces/provision` (привязать spreadsheet + owner)
- `POST /onboarding/wb-tokens` (добавить/обновить токен)
- `GET /onboarding/clients/{client_id}/status` (прогресс готовности)

Совместимость:
- маппинг полей к ожидаемым данным из `libs/BtlzApi/Code.js` (`email`, `roles`, `activation_link`, `spreadsheet_id`).

### Шаг 3. Подготовить модель данных onboarding-сервиса

Таблицы (минимум):
- `users` (email, name, status, activated_at)
- `invites` (token_hash, expires_at, invited_by, accepted_at, status)
- `user_workspaces` (user_id, spreadsheet_id, is_active)
- `provisioning_jobs` (idempotency_key, step, status, error_payload)
- `audit_events` (actor, action, target, ip, ua, created_at)

Критично:
- idempotency key на provisioning операции,
- уникальные ограничения для предотвращения дублей,
- soft-delete/деактивация вместо физического удаления в пользовательских сущностях.

### Шаг 4. Реализовать модуль приглашений

Функции:
- генерация одноразового invite token (в БД хранить только hash),
- TTL и лимит повторной отправки,
- шаблоны писем/сообщений,
- endpoint переотправки invite.

Минимальный delivery:
- сначала синхронная отправка + retry policy,
- затем вынос в очередь задач при росте нагрузки.

### Шаг 5. Реализовать активацию пользователя

После подтверждения invite:
- создание/активация user,
- выдача роли (`client` как дефолт),
- фиксация `activation_link used` в audit trail,
- блокировка повторного использования invite.

### Шаг 6. Встроить provisioning в текущий backend_compat

Оркестрация из `onboarding-service`:
1. `POST /api/admin/spreadsheets/register`
2. `POST /api/admin/wb/tokens/add`
3. `POST /api/ss/wb/token/get` (верификация)
4. `POST /api/ss/datasets/update` (smoke: accepted/200)

Требования к надежности:
- пошаговые retries с backoff,
- компенсация (rollback статусов) при неуспехе,
- полная трассировка в `provisioning_jobs`.

### Шаг 7. Закрыть базовые security-гепы перед массовым приглашением

Обязательно до production rollout:
- разделить service-to-service ключи (не один общий ключ на все операции),
- ограничить админ-роуты allowlist'ом сервисов,
- шифрование чувствительных токенов в хранилище,
- ротация секретов и журнал доступа к секретам,
- минимальный RBAC: `owner/admin/operator/client`.

### Шаг 8. Добавить операционную готовность (SRE baseline)

Нужно ввести:
- метрики onboarding-воронки (`invite_sent`, `invite_accepted`, `provision_success`, `provision_failed`),
- структурированные логи с `request_id`, `client_id`, `job_id`,
- алерты на рост ошибок provisioning и задержки,
- health/readiness probes для `onboarding-service`.

### Шаг 9. Ввести anti-abuse и multi-client guardrails

Минимум:
- rate limiting на создание invite и активацию,
- captcha/verification (если self-serve сценарий),
- ограничение попыток ввода/подтверждения,
- блокировка подозрительных источников.

### Шаг 10. Провести staged rollout

Порядок:
1. внутренние тестовые пользователи,
2. 3-5 пилотных клиентов,
3. расширение на весь поток.

Критерий перехода между стадиями:
- `provision_success_rate >= 98%`,
- `P95 time-to-active` в целевом окне,
- отсутствие критичных security инцидентов.

## 5) Проверки и критерии готовности

### Функциональные

- Unit: state machine, TTL invite, idempotency.
- Integration: полный путь `invite -> activate -> register spreadsheet -> add token -> validate`.
- Contract: ответы совместимы с ожиданиями `libs/BtlzApi/Code.js`.

### Нефункциональные

- Security checks: секреты, RBAC, доступ к админ-операциям.
- Load checks: пиковые приглашения и параллельный provisioning.
- Recovery checks: повтор шага после частичного сбоя не создает дублей.

### Бизнесовые

- Доля активации приглашений,
- Время до первого успешного обновления данных в таблице,
- Доля клиентов без ручного вмешательства оператора.

## 6) Риски

- Текущий `backend_compat` использует общий `X-Api-Key`; без сегментации ключей риск lateral access.
- Токены маркетплейса в SQLite в открытом виде требуют защиты перед масштабированием.
- В `libs/BtlzApi/Code.js` уже есть ожидание richer admin API; без слоя совместимости возможны интеграционные разрывы.
- При росте числа клиентов синхронные операции (в т.ч. внешние API) создадут очередь и деградацию onboarding latency.

## 7) Открытые вопросы (неблокирующие для старта MVP)

- Канал приглашений по умолчанию: email-only или multi-channel.
- Нужен ли self-serve onboarding или только операторский flow.
- SLA по времени активации клиента (target для `time-to-active`).
- Граница ответственности поддержки: кто обрабатывает `failed` provisioning.

## 8) Предлагаемый MVP-объем (минимум для запуска)

В MVP включить:
- invite lifecycle,
- user + workspace binding,
- provisioning оркестратор к существующим admin endpoint'ам,
- status endpoint,
- базовый audit + metrics + rate limiting.

Не включать в MVP:
- сложные роли и enterprise SSO,
- многоканальные кампании приглашений,
- глубокую BI-аналитику по воронке (достаточно базовых KPI).