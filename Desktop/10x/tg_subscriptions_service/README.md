# Telegram Subscriptions Service

MVP service for Telegram subscription sales and activation.

What it does:

- handles Telegram webhook updates;
- supports commands: `/start`, `/plans`, `/buy`, `/status`, `/support`, `/onboard`;
- creates Telegram invoices and handles `pre_checkout_query`;
- activates subscriptions on `successful_payment` (idempotent);
- stores customers, plans, payments, subscriptions, billing events in SQLite;
- optionally provisions workspace in `backend_compat` via `/onboard`.

What it does **not** do:

- does not manage Telegram channel access grants/revokes.

## Quick start

1) Create env variables:

```bash
copy .env.example .env
```

2) Set required values (`TG_BOT_TOKEN` at minimum).

3) Run service:

```bash
python main.py
```

Service starts on `http://TG_SVC_HOST:TG_SVC_PORT`.
Health check: `GET /health`.
Metrics: `GET /metrics` (requires `X-Admin-Token`).
Webhook path: `TG_WEBHOOK_PATH`.

## Commands

- `/start` - welcome and command list
- `/plans` - list available plans
- `/buy <plan_code>` - create invoice
- `/status` - active subscription status
- `/support` - support contact text from env
- `/onboard <email> <spreadsheet_id>` - connect client workspace in backend_compat (requires active paid subscription)

## Notes on payments

- For Telegram Stars use `currency = XTR`.
- `pre_checkout_query` is answered with validation of pending payload and amount/currency.
- Repeated `successful_payment` updates are idempotent by `telegram_payment_charge_id`.
- Repeated webhook deliveries are deduplicated by `update_id` before business logic.
- dedupe table is pruned in maintenance loop using `TG_WEBHOOK_DEDUPE_RETENTION_DAYS`.
- In production (`TG_ENV=production`) `TG_WEBHOOK_SECRET` is mandatory.

## Onboarding bridge

`/onboard` calls `backend_compat` endpoints in sequence:

1. `POST /admin/spreadsheets/register`
2. `POST /ss/wb/token/get`
3. `POST /ss/datasets/update`

WB token must be added through secure admin flow (server-side), not via Telegram chat.

Configure:

- `TG_BACKEND_API_BASE` (default: `http://127.0.0.1:8080/api`)
- one of `TG_BACKEND_API_KEY` or `TG_BACKEND_BEARER_TOKEN` is required for backend onboarding/admin calls
- `TG_HTTP_TIMEOUT_SECONDS` (default: `20`)
- `TG_HTTP_RETRIES` (default: `2`, max: `5`)
- `TG_WEBHOOK_DEDUPE_RETENTION_DAYS` (default: `30`)
- `TG_ENV` (`dev`/`production`; in production webhook secret is required)
- `TG_ADMIN_API_TOKEN` (required for secure admin endpoint)

### Secure admin WB token flow

WB tokens are accepted only via admin HTTP endpoint (not via Telegram commands/chat):

`POST /admin/wb-token`

Headers:

- `X-Admin-Token: <TG_ADMIN_API_TOKEN>`
- `Content-Type: application/json`

Body example:

```json
{
  "email": "owner@example.com",
  "spreadsheet_id": "sheet-123",
  "wb_token": "<WB_TOKEN>",
  "provision_after_add": true
}
```

Behavior:

- adds WB token through backend admin API;
- optionally runs provisioning (`register -> token/get -> datasets/update`);
- never requires WB token in Telegram chat.

## Persistence

SQLite file: `TG_SVC_DB_PATH`.
Tables are auto-created on startup.
