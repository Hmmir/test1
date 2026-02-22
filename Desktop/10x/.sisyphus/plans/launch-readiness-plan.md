# Launch Readiness Plan (Verifiable)

This checklist is pass/fail. Each item is complete only with command output evidence.

## P0 (Must pass before go-live)

1) Backend auth defaults are safe (no anonymous admin)
- Verify:
  - `python -m unittest discover -s backend_compat/tests -p "test_security_hardening.py" -v`
- Pass:
  - exit code is `0`
  - test `test_auth_context_denies_when_auth_is_not_configured` is `ok`

2) Tenant scope protection for `/api/ss/*`
- Verify:
  - `python -m unittest discover -s backend_compat/tests -p "test_security_hardening.py" -v`
- Pass:
  - test `test_spreadsheet_scope_blocks_cross_tenant_for_client` is `ok`

3) Onboarding requires paid entitlement
- Verify:
  - `python -m unittest discover -s tg_subscriptions_service/tests -p "test_onboarding_and_webhook_guardrails.py" -v`
- Pass:
  - `test_onboard_requires_active_subscription` and `test_onboard_provisions_when_subscription_is_active` are `ok`

4) Webhook idempotency by `update_id`
- Verify:
  - `python -m unittest discover -s tg_subscriptions_service/tests -p "test_onboarding_and_webhook_guardrails.py" -v`
- Pass:
  - `test_storage_register_webhook_update_is_idempotent` and `test_service_deduplicates_by_update_id` are `ok`

5) Production requires webhook secret
- Verify:
  - `python -m unittest discover -s tg_subscriptions_service/tests -p "test_onboarding_and_webhook_guardrails.py" -v`
- Pass:
  - `test_webhook_secret_required_in_production` is `ok`

6) Collector concurrency lock is enforced
- Verify:
  - `python -m unittest discover -s backend_compat/tests -p "test_security_hardening.py" -v`
- Pass:
  - `test_collector_lock_conflict_and_release` is `ok`

7) Full regression gate
- Verify:
  - `python -m unittest discover -s backend_compat/tests -p "test_*.py"`
  - `python -m unittest discover -s tg_subscriptions_service/tests -p "test_*.py"`
- Pass:
  - both commands exit with code `0`

## P1 (Operational gates in first week)

1) Health and metrics endpoint checks
- Verify:
  - `curl -s -o backend_health.json -w "%{http_code}" http://127.0.0.1:8080/api/health`
  - `curl -s -o backend_metrics.json -w "%{http_code}" -H "Authorization: Bearer <TOKEN>" http://127.0.0.1:8080/api/admin/metrics`
  - `curl -s -o tg_health.json -w "%{http_code}" http://127.0.0.1:8090/health`
  - `curl -s -o tg_metrics.json -w "%{http_code}" -H "X-Admin-Token: <TG_ADMIN_API_TOKEN>" http://127.0.0.1:8090/metrics`
- Pass:
  - all four commands print HTTP status `200`
  - `backend_metrics.json` contains `http_auth_failures_total`, `http_rate_limited_total`, `http_onboarding_failures_total`
  - `tg_metrics.json` contains `tg_webhook_auth_failures_total`, `tg_onboarding_failures_total`, `tg_webhook_duplicates_total`

2) Backup and restore drill
- Verify:
  - run backup and restore commands from `DEPLOYMENT_RUNBOOK.md`
  - `sqlite3 <db> "PRAGMA integrity_check;"`
- Pass:
  - integrity check returns `ok`
  - services come back healthy after restore

3) Startup order and service status
- Verify:
  - `systemctl is-active 10x-backend.service 10x-tg-subscriptions.service 10x-collector.service`
- Pass:
  - all report `active`

## P2 (Post-launch hardening)

1) Alerts wired in monitoring
- Verify:
  - `python -c "from pathlib import Path; text=Path('monitoring/alerts/prometheus-rules.yml').read_text(encoding='utf-8'); req=['BackendAuthFailuresSpike','BackendRateLimitSpike','BackendOnboardingFailures','TelegramWebhookAuthFailures','TelegramOnboardingFailures']; missing=[r for r in req if r not in text]; print({'missing':missing}); raise SystemExit(1 if missing else 0)"`
  - `curl -s "http://127.0.0.1:9090/api/v1/rules" -o prometheus_rules.json`
- Pass:
  - first command exits with code `0`
  - `prometheus_rules.json` contains all five alert names above

2) Performance baseline
- Verify:
  - `python tools/http_load_probe.py --url http://127.0.0.1:8080/api/health --url http://127.0.0.1:8090/health --requests 300 --concurrency 30 --max-error-rate 0.01 --max-p95-ms 800`
  - `python tools/http_load_probe.py --url http://127.0.0.1:8080/api/ss/wb/token/get --method POST --headers-json '{"Authorization":"Bearer <TOKEN>"}' --body-json '{"spreadsheet_id":"<SHEET_ID>"}' --requests 120 --concurrency 12 --max-error-rate 0.02 --max-p95-ms 1200`
- Pass:
  - both commands exit with code `0`
  - JSON output includes `database_locked_errors: 0`
