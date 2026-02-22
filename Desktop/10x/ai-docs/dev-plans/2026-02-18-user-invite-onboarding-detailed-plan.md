# Детальный план: микросервис приглашения пользователей и готовность продукта

Дата: 2026-02-18
Проект: `C:\Users\alien\Desktop\10x`

## 0) Статус planning skills и fallback

### 0.1 Что проверено

- Попытка загрузить через `skill`:
  - `shared-base-rules`
  - `shared-docs-paths`
  - `planning-base`
  Результат: skills не зарегистрированы в runtime (доступны только `playwright`, `frontend-ui-ux`, `git-master`, `dev-browser`, `ccs-delegation`).
- На машине обнаружены локальные каталоги skill-материалов:
  - `C:\Users\alien\.opencode\skills\shared\base-rules\SKILL.md`
  - `C:\Users\alien\.opencode\skills\shared\docs-paths\SKILL.md`
  - `C:\Users\alien\.opencode\skills\planning\base\SKILL.md`
- Попытка задействовать эти skills через `task(... load_skills=[...])` также показала, что они недоступны активному реестру навыков этой сессии.

### 0.2 Практический вывод

- "Скачать"/подключить указанные planning skills в текущий runtime без смены конфигурации окружения нельзя.
- Безопасный fallback: использовать детальный явный план на основе фактов репозитория и текущего API-контракта.

---

## 1) Базовый анализ текущего состояния проекта

## 1.1 Архитектура сейчас

- API-слой: `backend_compat/server.py` (Python `http.server`, ThreadingHTTPServer).
- Хранилище: SQLite через `backend_compat/storage.py`.
- Текущий onboarding клиента выполняется вручную через:
  - `POST /api/admin/spreadsheets/register`
  - `POST /api/admin/wb/tokens/add`
  - `POST /api/ss/wb/token/get`
- Общая авторизация: один `X-Api-Key` (`BTLZ_API_KEY`) без пользовательских ролей и сессий.

## 1.2 Технические сигналы, важные для нового сервиса

- В данных уже есть `owner_email` в `spreadsheets`, но нет invite lifecycle.
- Нет таблиц и API для пользователей/ролей/приглашений/активаций/аудита.
- В Apps Script библиотеке есть ожидание richer admin API (`/admin/user/...`, `activation_link`) в `libs/BtlzApi/Code.js`.
- В `backend_compat/README.md` прямо зафиксирована необходимость auth + rate limiting + audit logs для multi-client.

## 1.3 Главный архитектурный gap

Система умеет обслуживать подключенного клиента, но не имеет безопасного, повторяемого, наблюдаемого и масштабируемого процесса "пригласить -> активировать -> подключить -> проверить".

---

## 2) Цели и критерии успеха

## 2.1 Цели

1. Ввести управляемый onboarding поток для новых клиентов.
2. Убрать ручные и нестабильные шаги подключения.
3. Подготовить продукт к multi-client эксплуатации по безопасности и операционным практикам.

## 2.2 KPI/SLI для этапа запуска

- `invite_accept_rate >= 60%` (для warm leads).
- `provision_success_rate >= 98%`.
- `P95 time_to_active <= 20 мин`.
- `manual_intervention_rate <= 10%`.
- `critical_security_findings = 0` перед rollout на внешний трафик.

---

## 3) Scope / Non-scope

## 3.1 В scope (MVP + readiness)

- Invite lifecycle.
- Активация пользователя.
- Привязка пользователя к spreadsheet.
- Подключение WB токена с проверкой.
- Статусы onboarding и журнал действий.
- Базовый RBAC, rate limiting, audit trail, метрики.

## 3.2 Вне scope первого релиза

- Enterprise SSO/SAML/OIDC federation.
- Многоэтапные approval workflows.
- Self-serve billing и advanced тенантинг.
- Полноценная CRM-автоматизация кампаний приглашений.

---

## 4) Целевая архитектура (минимально-инвазивная)

## 4.1 Компоненты

1. `onboarding-service` (новый сервис):
   - API приглашений и статусов.
   - Оркестратор provisioning шагов.
   - Хранилище onboarding-сущностей.
2. `backend_compat` (текущий сервис):
   - источник данных и существующие бизнес-эндпоинты.
3. Notification adapter:
   - отправка invite-ссылок (email/webhook/оператор).
4. Observability stack:
   - метрики + structured logs + алерты.

## 4.2 Принцип интеграции

- Новый сервис не ломает текущий `backend_compat` и использует его существующие endpoint'ы как подложку provisioning.
- Контракт `libs/BtlzApi/Code.js` закрывается постепенно через onboarding API и/или shim layer.

---

## 5) State machine onboarding

Состояния клиента:

1. `invited`
2. `email_confirmed`
3. `workspace_registered`
4. `token_added`
5. `validated`
6. `active`

Терминальные ошибки:

- `failed`
- `expired`
- `revoked`

## 5.1 Разрешенные переходы

- `invited -> email_confirmed`
- `email_confirmed -> workspace_registered`
- `workspace_registered -> token_added`
- `token_added -> validated`
- `validated -> active`
- Любое состояние -> `failed` при неустранимой ошибке
- `invited -> expired` по TTL
- `invited|email_confirmed -> revoked` администратором

## 5.2 Требования к переходам

- Каждый переход идемпотентен.
- Каждый переход пишет audit event.
- Для повторных вызовов возвращается последняя консистентная стадия.

---

## 6) Контракт API onboarding-service (детализация)

## 6.1 POST `/onboarding/invites`

Назначение: создать приглашение.

Request:

```json
{
  "email": "client@example.com",
  "name": "Client Name",
  "roles": ["client"],
  "expires_in_hours": 72,
  "idempotency_key": "inv-2026-..."
}
```

Response:

```json
{
  "success": true,
  "invite_id": "inv_123",
  "activation_link": "https://.../accept?token=...",
  "status": "invited",
  "expires_at": "2026-02-21T10:00:00Z"
}
```

## 6.2 POST `/onboarding/invites/{token}/accept`

Назначение: подтверждение приглашения.

Request:

```json
{
  "password": "optional-or-empty",
  "accept_terms": true
}
```

Response:

```json
{
  "success": true,
  "user_id": "usr_456",
  "status": "email_confirmed"
}
```

## 6.3 POST `/onboarding/workspaces/provision`

Назначение: связать user и spreadsheet + запустить provisioning.

Request:

```json
{
  "user_id": "usr_456",
  "spreadsheet_id": "1AbC...",
  "owner_email": "client@example.com",
  "wb_token": "eyJ...",
  "idempotency_key": "prov-2026-..."
}
```

Response:

```json
{
  "success": true,
  "job_id": "job_789",
  "status": "workspace_registered"
}
```

## 6.4 GET `/onboarding/clients/{client_id}/status`

Response:

```json
{
  "success": true,
  "client_id": "usr_456",
  "status": "token_added",
  "last_error": null,
  "updated_at": "2026-02-18T13:05:00Z"
}
```

## 6.5 POST `/onboarding/wb-tokens/rotate`

Назначение: безопасная ротация токена.

Request:

```json
{
  "client_id": "usr_456",
  "wb_token": "eyJ...new",
  "reason": "scheduled_rotation"
}
```

Response:

```json
{
  "success": true,
  "status": "token_added"
}
```

---

## 7) Модель данных onboarding-service

## 7.1 Таблицы

1. `users`
   - `id`, `email`, `name`, `status`, `roles_json`, `activated_at`, `created_at`, `updated_at`
2. `invites`
   - `id`, `email`, `token_hash`, `status`, `expires_at`, `invited_by`, `accepted_at`, `created_at`
3. `user_workspaces`
   - `id`, `user_id`, `spreadsheet_id`, `owner_email`, `is_active`, `created_at`, `updated_at`
4. `provisioning_jobs`
   - `id`, `user_id`, `workspace_id`, `idempotency_key`, `step`, `status`, `attempt`, `error_json`, `created_at`, `updated_at`
5. `audit_events`
   - `id`, `actor_id`, `actor_type`, `action`, `target_type`, `target_id`, `meta_json`, `ip`, `user_agent`, `created_at`

## 7.2 Ограничения

- `users.email` UNIQUE.
- `invites.token_hash` UNIQUE.
- `user_workspaces(user_id, spreadsheet_id)` UNIQUE.
- `provisioning_jobs.idempotency_key` UNIQUE.

## 7.3 Политика хранения

- Токены в открытом виде не хранить.
- Хранить только encrypted secret + suffix для отладки.
- Audit events хранить минимум 180 дней.

---

## 8) Provisioning orchestration (детально)

Job steps:

1. `register_spreadsheet`
   - `POST /api/admin/spreadsheets/register`
   - при успехе: `workspace_registered`
2. `add_wb_token`
   - `POST /api/admin/wb/tokens/add`
   - при успехе: `token_added`
3. `verify_token`
   - `POST /api/ss/wb/token/get`
   - проверка: есть активный token + sid
4. `smoke_update`
   - `POST /api/ss/datasets/update` с минимальным payload
   - проверка: HTTP 200, `success=true` или ожидаемый `accepted`
5. `mark_active`
   - перевод клиента в `active`

## 8.1 Retry policy

- Retry: `max_attempts=5`, `exponential backoff` (2s, 5s, 10s, 20s, 40s).
- На 4xx (кроме 429) ретраев нет.
- На 429/5xx/timeout ретраи обязательны.

## 8.2 Компенсации

- Если шаг 2 упал после шага 1: статус `failed`, но workspace остается зарегистрированным (для ручного восстановления).
- Если шаг 4 упал: клиент не переводится в `active`, остается `validated_pending`/`failed` с кодом ошибки.

---

## 9) Security baseline (до массового трафика)

1. Разделить ключи:
   - `ONBOARDING_TO_BACKEND_API_KEY`
   - `OPERATOR_API_KEY`
2. Ограничить доступ к admin endpoint'ам по IP allowlist/mTLS (если доступно).
3. Ввести RBAC:
   - `owner`, `admin`, `operator`, `client`.
4. Шифрование секретов:
   - envelope encryption (master key в секрет-менеджере).
5. Rate limiting:
   - invite create: 10 req/min на actor.
   - invite accept: 5 req/min на IP/email.
6. Audit logging:
   - все admin/onboarding операции только с `request_id`.

---

## 10) Наблюдаемость и эксплуатация

## 10.1 Метрики

- `onboarding_invites_total{status}`
- `onboarding_accept_total{status}`
- `onboarding_provision_jobs_total{step,status}`
- `onboarding_time_to_active_seconds` (histogram)
- `onboarding_failures_total{reason}`

## 10.2 Логи

JSON-лог со структурой:

- `timestamp`
- `level`
- `request_id`
- `user_id`
- `workspace_id`
- `job_id`
- `action`
- `status`
- `error_code`

## 10.3 Алерты

- `provision_success_rate < 95%` за 15 минут.
- `P95 time_to_active` выше SLO.
- Рост `429/5xx` от backend_compat.

---

## 11) Детальный поэтапный план внедрения

## Этап A (1-2 дня): каркас и контракты

- Утвердить state machine и API contract.
- Подготовить OpenAPI spec.
- Описать error codes и idempotency semantics.

Результат этапа:
- frozen API schema и согласованные статусы.

## Этап B (2-3 дня): data layer

- Создать БД onboarding-сервиса + миграции.
- Реализовать repository слой и ограничения уникальности.
- Подготовить базовый audit writer.

Результат этапа:
- стабильный persistence с идемпотентностью.

## Этап C (3-4 дня): invite + activation

- Реализовать create invite / accept invite.
- Внедрить TTL, single-use token, resend logic.
- Подключить notification adapter (минимум email stub + провайдер).

Результат этапа:
- рабочий invite lifecycle.

## Этап D (3-4 дня): provisioning orchestration

- Реализовать job engine по 5 шагам.
- Интегрировать `backend_compat` endpoints.
- Реализовать retry/backoff/compensation.

Результат этапа:
- автоматический перевод клиента в `active` при успешных проверках.

## Этап E (2-3 дня): security + rate limit + observability

- RBAC middleware.
- Шифрование секретов.
- Rate limiting и anti-abuse rules.
- Метрики, structured logs, алерты.

Результат этапа:
- readiness к pilot нагрузке.

## Этап F (2-3 дня): QA + rollout

- Интеграционные тесты полного флоу.
- Pilot 3-5 клиентов.
- Пост-пилот фиксы и масштабирование.

Результат этапа:
- controlled production rollout.

---

## 12) План тестирования

## 12.1 Unit

- state transitions.
- token hash/ttl/expiry.
- idempotency behavior.
- retry decision matrix.

## 12.2 Integration

- mock backend_compat 200/400/429/500 сценарии.
- end-to-end `invite -> active`.
- повторный запуск того же provisioning запроса.

## 12.3 Security

- доступ к admin endpoint'ам без роли.
- brute-force на invite accept.
- утечки секретов в логах.

## 12.4 Smoke в staging

- 1 реальный spreadsheet.
- 1 валидный WB token.
- Проверка `/api/ss/wb/token/get` и `/api/ss/datasets/update` после onboarding.

---

## 13) Rollout стратегия

1. `internal-only` (команда/операторы).
2. `pilot` (3-5 клиентов).
3. `limited GA` (до 20% потока).
4. `full GA`.

Gate на переход стадии:

- успех по KPI предыдущей стадии.
- нет open critical инцидентов.
- подтверждена работоспособность rollback.

Rollback:

- выключение create-invite endpoint feature-flag.
- перевод новых заявок в manual onboarding.
- сохранение audit trail и job истории.

---

## 14) Риски и меры

1. Риск: единый API key в legacy backend.
   - Мера: отдельные ключи + allowlist + периодическая ротация.
2. Риск: хранение маркетплейс токенов в открытом виде.
   - Мера: encrypted-at-rest + redaction в логах.
3. Риск: нестабильность внешних API WB.
   - Мера: retry/backoff/circuit-breaker + статус `retryable_failed`.
4. Риск: расхождение ожиданий Apps Script admin контракта.
   - Мера: compatibility shim и contract tests против `libs/BtlzApi/Code.js`.

---

## 15) Definition of Done

- Реализован полный onboarding flow до `active` без ручных SQL-операций.
- Есть идемпотентный provisioning engine с логируемыми шагами.
- Все критичные действия покрыты audit trail.
- Включены rate limiting и минимум RBAC.
- Подготовлены dashboards/alerts и runbook.
- Pilot stage прошел по KPI и без критичных security дефектов.