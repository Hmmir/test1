# Deployment Runbook

This runbook describes production deployment for:

- `backend_compat/server.py`
- `tg_subscriptions_service/main.py`
- `backend_compat/collector_daemon.py`

## 1) Pre-flight

1. Install Python 3.10+.
2. Install dependencies:
   - base dependencies used by your project
   - optional encryption dependency:

```bash
pip install -r backend_compat/requirements.optional.txt
```

3. Prepare env files (example paths):
   - `/etc/10x/backend.env`
   - `/etc/10x/tg.env`
   - `/etc/10x/collector.env`

Required security vars:

- backend: `BTLZ_API_KEY` or `BTLZ_BEARER_TOKENS_JSON`
- tg service: `TG_ENV=production`, `TG_WEBHOOK_SECRET`, `TG_ADMIN_API_TOKEN`
- tg -> backend auth for onboarding/admin bridge: one of `TG_BACKEND_API_KEY` or `TG_BACKEND_BEARER_TOKEN`
- backend token encryption (recommended): `BTLZ_TOKEN_ENCRYPTION_KEY`
- collector lock safety: `BTLZ_COLLECTOR_LOCK_TTL_SECONDS` (same value in backend and collector env)

## 2) Startup order

Use this order:

1. `backend_compat` API
2. `tg_subscriptions_service`
3. `collector_daemon`

Reason: tg onboarding and collector both depend on backend availability.

## 3) systemd units

Assume repo path `/opt/10x` and virtualenv `/opt/10x/.venv`.

### `/etc/systemd/system/10x-backend.service`

```ini
[Unit]
Description=10x backend_compat API
After=network.target

[Service]
Type=simple
WorkingDirectory=/opt/10x/backend_compat
EnvironmentFile=/etc/10x/backend.env
ExecStart=/opt/10x/.venv/bin/python server.py
Restart=always
RestartSec=3
User=www-data
Group=www-data

[Install]
WantedBy=multi-user.target
```

### `/etc/systemd/system/10x-tg-subscriptions.service`

```ini
[Unit]
Description=10x Telegram subscriptions service
After=network.target 10x-backend.service
Requires=10x-backend.service

[Service]
Type=simple
WorkingDirectory=/opt/10x/tg_subscriptions_service
EnvironmentFile=/etc/10x/tg.env
ExecStart=/opt/10x/.venv/bin/python main.py
Restart=always
RestartSec=3
User=www-data
Group=www-data

[Install]
WantedBy=multi-user.target
```

### `/etc/systemd/system/10x-collector.service`

```ini
[Unit]
Description=10x collector daemon
After=network.target 10x-backend.service
Requires=10x-backend.service

[Service]
Type=simple
WorkingDirectory=/opt/10x/backend_compat
EnvironmentFile=/etc/10x/collector.env
ExecStart=/opt/10x/.venv/bin/python collector_daemon.py
Restart=always
RestartSec=5
User=www-data
Group=www-data

[Install]
WantedBy=multi-user.target
```

Enable and start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now 10x-backend.service
sudo systemctl enable --now 10x-tg-subscriptions.service
sudo systemctl enable --now 10x-collector.service
```

## 4) Health and metrics checks

Backend:

```bash
curl http://127.0.0.1:8080/api/health
curl -H "Authorization: Bearer <TOKEN>" http://127.0.0.1:8080/api/admin/metrics
```

Telegram service:

```bash
curl http://127.0.0.1:8090/health
curl -H "X-Admin-Token: <TG_ADMIN_API_TOKEN>" http://127.0.0.1:8090/metrics
```

## 5) SQLite backup

Primary DB files:

- backend: `backend_compat/data/btlz.db`
- tg service: `tg_subscriptions_service/data/tg_subscriptions.db`

### Recommended (online backup with sqlite3)

```bash
sqlite3 /opt/10x/backend_compat/data/btlz.db ".backup '/var/backups/10x/btlz-$(date +%F-%H%M%S).db'"
sqlite3 /opt/10x/tg_subscriptions_service/data/tg_subscriptions.db ".backup '/var/backups/10x/tg-$(date +%F-%H%M%S).db'"
```

### Safer maintenance backup (stop services first)

```bash
sudo systemctl stop 10x-collector.service 10x-tg-subscriptions.service 10x-backend.service
cp /opt/10x/backend_compat/data/btlz.db /var/backups/10x/
cp /opt/10x/tg_subscriptions_service/data/tg_subscriptions.db /var/backups/10x/
sudo systemctl start 10x-backend.service 10x-tg-subscriptions.service 10x-collector.service
```

## 6) SQLite restore

1. Stop services.
2. Restore DB file from backup.
3. Start services in startup order.
4. Verify health and metrics endpoints.

Example:

```bash
sudo systemctl stop 10x-collector.service 10x-tg-subscriptions.service 10x-backend.service
cp /var/backups/10x/btlz-YYYY-MM-DD-HHMMSS.db /opt/10x/backend_compat/data/btlz.db
cp /var/backups/10x/tg-YYYY-MM-DD-HHMMSS.db /opt/10x/tg_subscriptions_service/data/tg_subscriptions.db
sudo systemctl start 10x-backend.service
sudo systemctl start 10x-tg-subscriptions.service
sudo systemctl start 10x-collector.service
```

## 7) Secure admin WB token procedure

Do not send WB token in Telegram messages.

Use only:

- `POST /admin/wb-token` on tg service with `X-Admin-Token`
- payload: `email`, `spreadsheet_id`, `wb_token`

This keeps token handling in server-to-server admin flow.
