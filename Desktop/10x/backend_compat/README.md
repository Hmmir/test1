# Own Backend (compat layer)

This folder is a compatibility backend for the cloned Google Sheets + Apps Script solution.

It gives you a private API surface so each client can provide their own Wildberries token, and their spreadsheet can refresh from your own infrastructure.

## What is implemented now

- HTTP API server with required routes:
  - `POST /api/ss/datasets/data`
  - `POST /api/ss/datasets/upload`
  - `POST /api/ss/datasets/update`
  - `POST /api/ss/wb/token/get`
  - `POST /api/actions`
- Admin routes to onboard spreadsheets and WB tokens:
  - `POST /api/admin/spreadsheets/register`
  - `POST /api/admin/wb/tokens/add`
  - `POST /api/admin/wb/tokens/list`
- SQLite persistence:
  - spreadsheet registry
  - WB tokens per spreadsheet
  - action plan storage (`wbActionPlan*`)
  - plan-month save sink (`wb10xPlanMonthSave`)

## Current status

- `wbActionPlan`, `wbActionPlanUpload`, `wbActionPlanDelete`, `wb10xPlanMonthSave` are wired to storage.
- `wbCardsData_v1` can fetch SKU cards from Wildberries Content API using stored WB tokens.
- `wb10xMain_planMonth_v1`, `wb10xSalesFinReportTotal_v1`, `wb10xSalesReport_v1/v2`, `wb10xAnalyticsData_v1` are now fed by WB statistics data where available (orders/sales/stocks + cards metadata).
- Financial decomposition fields still use simplified math (many cost components are zero until you implement your own economics model).
- `actions` endpoint supports `get_tech_sheets_list`, `get_tech_sheets_editors`, `processGroupCalculations`, `search/positions`, and generic action passthrough.
- `actions` endpoint also supports:
  - `wb/adv/v1/upd/insert` (fetch adv spend updates; optionally write to a sheet via Google SA)
  - `sheets/wb-plus/format-rules/reset` (dedupe conditional formatting rules via Google SA)
  - `search/keys/refresh` (build top search keys from adv normquery stats; optionally write to `search_keys` sheet)
  - `search/keys/upsert` (manual load of search keys into local DB)
  - `search/positions/collect` (parse positions for stored keys and save to `daily_positions`)
  - `collector/run` (collect daily snapshots into local DB for autonomous historical parity)

- Autonomous snapshot storage (SQLite):
  - `daily_prices` (price + spp)
  - `daily_stocks` (stocks + in-way)
  - `daily_adv` (daily ad spend split by type)
  - `daily_funnel` (open card/cart/wishlist)
  - `daily_localization` (regional orders/localization model)
  - `daily_positions` (search key positions)
  - `search_keys` (persisted keys per SKU)

## Run locally

```bash
python server.py
```

Optional dependency (needed only when token encryption is enabled):

```bash
pip install -r requirements.optional.txt
```

Environment variables:

- `BTLZ_HOST` (default: `127.0.0.1`)
- `BTLZ_PORT` (default: `8080`)
- `BTLZ_DB_PATH` (default: `./data/btlz.db`)
- `BTLZ_API_KEY` (optional, accepts `X-Api-Key` and `Authorization: Bearer <same_key>`)
- `BTLZ_BEARER_TOKENS_JSON` (optional JSON map for role-based bearer auth; supports tenant scope via `spreadsheets`, example: `{"token_admin":{"role":"admin"},"token_client":{"role":"client","spreadsheets":["sheet-1"]}}`)
- `BTLZ_TECH_SHEETS_LIST` (comma-separated list for protection flow)
- `BTLZ_TECH_SHEETS_EDITORS` (comma-separated editor emails)
- `BTLZ_UPSTREAM_BASE` (default: `https://mp.btlz-api.ru/api`)
- `BTLZ_UPSTREAM_SOURCE_SHEET_ID` (default: source sheet from clone)
- `BTLZ_UPSTREAM_ENABLED` (`1`/`0`, default: `0`)
- `BTLZ_ANALYTICS_FILL_MISSING_DAYS` (`1`/`0`, default: `1`)
- `BTLZ_ANALYTICS_MIN_OPEN_CARD` (default: `0`)
- `BTLZ_ADV_DAILY_ENABLED` (`1`/`0`, default: `1`)
- `BTLZ_ADV_DAILY_MAX_DAYS` (default: `120`)
- `BTLZ_ADV_SEARCH_TYPE_CODES` (default: `6,7,8,9`)
- `BTLZ_ADV_AUTO_TYPE_CODES` (default: `4,5`)
- `BTLZ_ADV_AUTO_BID_TYPES` (default: `unified`)
- `BTLZ_ADV_SEARCH_BID_TYPES` (default: `manual`)
- `BTLZ_GOOGLE_SA_JSON` (service account JSON content; optional)
- `BTLZ_GOOGLE_SA_FILE` (path to service account JSON file; optional)
- `BTLZ_USE_STORED_DAILY_ADV` (`1`/`0`, default: `1`)
- `BTLZ_USE_STORED_DAILY_STOCKS` (`1`/`0`, default: `1`)
- `BTLZ_USE_STORED_DAILY_PRICES` (`1`/`0`, default: `1`)
- `BTLZ_USE_STORED_DAILY_FUNNEL` (`1`/`0`, default: `1`)
- `BTLZ_USE_STORED_DAILY_LOCALIZATION` (`1`/`0`, default: `1`)
- `BTLZ_WB_SEARCH_DEST` (default: `-1257786`, single request dest; may contain comma-separated dest list)
- `BTLZ_WB_SEARCH_DESTS` (comma-separated dest list for multi-request mode, default: uses `BTLZ_WB_SEARCH_DEST` as a single entry)
- `BTLZ_WB_SEARCH_POS_AGG` (default: `first`, options: `first`,`first_nonzero`,`best`,`min`,`median`)
- `BTLZ_WB_SEARCH_SITE_DEFAULTS` (`1`/`0`, default: `0`, when enabled adds common WB web/mobile params like `regions`/`couponsGeo`)
- `BTLZ_WB_SEARCH_LOCALE` (default: `ru`)
- `BTLZ_WB_SEARCH_REGIONS` (optional, CSV string)
- `BTLZ_WB_SEARCH_COUPONS_GEO` (optional, CSV string)
- `BTLZ_WB_SEARCH_EMP` (optional, default filled by site defaults)
- `BTLZ_WB_SEARCH_LOCATE` (optional, default filled by site defaults)
- `BTLZ_WB_SEARCH_REG` (optional, default filled by site defaults)
- `BTLZ_WB_SEARCH_SPP_FIX_GEO` (optional, default filled by site defaults)
- `BTLZ_WB_SEARCH_PRICE_MARGIN_COEFF` (optional, default filled by site defaults)
- `BTLZ_WB_PUBLIC_DELAY_SECONDS` (default: `0`, add delay between public search requests to reduce 429)
- `BTLZ_WB_PUBLIC_CACHE_ENABLED` (default: `1`)
- `BTLZ_WB_PUBLIC_CACHE_TTL_SECONDS` (default: `BTLZ_WB_CACHE_TTL_SECONDS`)
- `BTLZ_RATE_LIMIT_WINDOW_SECONDS` (default: `60`)
- `BTLZ_RATE_LIMIT_ADMIN_PER_WINDOW` (default: `120`)
- `BTLZ_RATE_LIMIT_ACTIONS_PER_WINDOW` (default: `240`)
- `BTLZ_RATE_LIMIT_DATA_PER_WINDOW` (default: `1200`)
- `BTLZ_COLLECTOR_LOCK_TTL_SECONDS` (default: `1800`, shared lock TTL for collector daemon and `/actions` collector run)
- `BTLZ_AUDIT_ENABLED` (`1`/`0`, default: `1`)
- `BTLZ_TOKEN_ENCRYPTION_KEY` (optional Fernet key to encrypt WB tokens at rest)
- if `BTLZ_TOKEN_ENCRYPTION_KEY` is set, install optional deps: `pip install -r requirements.optional.txt`
- `BTLZ_QUALITY_GATES_ENABLED` (`1`/`0`, default: `1`)
- `BTLZ_QUALITY_GATES_FILE` (default: `backend_compat/data/dataset_quality_gates.json`)
- `BTLZ_QUALITY_GATES_DEFAULT_UPSTREAM` (`1`/`0`, default: `1`)

Token encryption migration notes:

- generate a Fernet key once: `python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"`
- set `BTLZ_TOKEN_ENCRYPTION_KEY` and restart backend
- legacy plaintext WB tokens are re-encrypted lazily on first token read (`/api/admin/wb/tokens/list` or `/api/ss/wb/token/get`)
- verify migration in SQLite:
  - `SELECT COUNT(*) FROM wb_tokens WHERE token LIKE 'enc:v1:%';`
  - `SELECT COUNT(*) FROM wb_tokens;`
  - encrypted count should match total count after warm-up

Upstream proxy behavior:

- for dataset names used by Apps Script (`wbCardsData_v1`, `wb10xMain_planMonth_v1`,
  `wb10xSalesFinReportTotal_v1`, `wbJamClusters_v1`, `wb10xAnalyticsData_v1`,
  `wb10xSalesReport_v1/v2`, `wb10xAdvNormqueryStatsByDatesNmIdsAdvertIds_v1`) the
  backend first tries upstream for exact payload/shape parity, then falls back to local
  WB-based builders if upstream is unavailable.
- checklist endpoint `GET /api/ss/{spreadsheet_id}/dataset/wb/checklist` follows the
  same rule: upstream first, local fallback.
- quality gates can override upstream routing per dataset via `BTLZ_QUALITY_GATES_FILE`
  (see `backend_compat/data/dataset_quality_gates.example.json`).

Health check:

```bash
curl http://127.0.0.1:8080/api/health
```

Metrics snapshot (requires auth with API key/bearer and operator role):

```bash
curl -X GET http://127.0.0.1:8080/api/admin/metrics \
  -H "Authorization: Bearer <TOKEN>"
```

Current counters include request totals, auth failures, forbidden responses,
rate-limit rejections, and onboarding success/failure totals.

Tenant scope and token exposure rules:

- all `/api/ss/*` routes require a registered active spreadsheet in `spreadsheets` table;
- `client` bearer tokens are restricted to `spreadsheets` declared in `BTLZ_BEARER_TOKENS_JSON`;
- `/api/ss/wb/token/get` returns raw `token` only for operator/admin roles;
- for client role it returns only `sid`, `analytics`, `token_suffix`.

Collector CLI (recommended cron job, 1-4 times/day):

```bash
python collector.py --spreadsheet-id <SHEET_ID> --date-from 2026-02-14 --date-to 2026-02-14
```

Collector daemon (long-running scheduler):

```bash
BTLZ_COLLECTOR_SHEET_ID=<SHEET_ID> python collector_daemon.py
```

Daemon env:

- `BTLZ_COLLECTOR_SHEET_ID` (required)
- `BTLZ_COLLECTOR_INTERVAL_MIN` (default: `360`)
- `BTLZ_COLLECTOR_WITH_POSITIONS` (`1`/`0`, default: `0`)
- `BTLZ_COLLECTOR_WITH_FUNNEL` (`1`/`0`, default: `1`)
- `BTLZ_COLLECTOR_NM_IDS` (optional CSV)
- `BTLZ_COLLECTOR_ONCE` (`1`/`0`, default: `0`)
- `BTLZ_COLLECTOR_LOCK_TTL_SECONDS` (default: `1800`)

Collector concurrency protection:

- daemon and API action `collector/run` share one DB lock;
- if a run is already active, parallel run returns `success=false` with `message="collector is already running"`.

Collector via API action:

```bash
curl -X POST http://127.0.0.1:8080/api/actions \
  -H "Authorization: Bearer <TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{"action":"collector/run","ssId":"<SHEET_ID>","dateFrom":"2026-02-14","dateTo":"2026-02-14"}'
```

SEO tuning (search positions dest/agg):

```bash
python search_positions_tune.py --xlsx ../clone_export_latest.xlsx --sheet search_positions --reduce last --mode multi
```

## Onboard a new client spreadsheet

1) Register spreadsheet:

```bash
curl -X POST http://127.0.0.1:8080/api/admin/spreadsheets/register \
  -H "Authorization: Bearer <TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{"spreadsheet_id":"<SHEET_ID>","owner_email":"client@example.com"}'
```

2) Save WB token:

```bash
curl -X POST http://127.0.0.1:8080/api/admin/wb/tokens/add \
  -H "Authorization: Bearer <TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{"spreadsheet_id":"<SHEET_ID>","token":"<WB_JWT_TOKEN>"}'
```

3) Verify token mapping:

```bash
curl -X POST http://127.0.0.1:8080/api/admin/wb/tokens/list \
  -H "Authorization: Bearer <TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{"spreadsheet_id":"<SHEET_ID>"}'
```

## Next implementation steps for full parity

1. Implement data builders for:
   - `wb10xSalesReport_v1`, `wb10xSalesReport_v2`
   - `wb10xSalesFinReportTotal_v1`
   - `wb10xMain_planMonth_v1`
   - `wbJamClusters_v1`
   - `wb10xAdvNormqueryStatsByDatesNmIdsAdvertIds_v1`
   - `wb10xAnalyticsData_v1`
2. Replace hardcoded external Apps Script web-app URLs in the cloned script with your own backend actions endpoint.
3. Move cloned libraries to your own Apps Script library projects and update library IDs in manifest.
4. Add auth + rate limiting + audit logs for multi-client operation.

## Apps Script ownership bootstrap

The repository includes `bootstrap_own_apps_script_stack.py` in project root. It:

- publishes owned copies of `MenuParser`, `DB`, `BtlzApi`, `common10x`, `utilities10X`
- rewires inter-library dependencies to your owned IDs
- rewires the cloned main script manifest to your owned libraries
- updates local `.clasp.json` files and writes `own_stack_result.json`

## Local test without permanent public domain

Apps Script runs on Google servers, so it cannot call `http://localhost` directly. For local testing, run a temporary HTTPS tunnel.

Example with Cloudflare Tunnel:

```bash
cloudflared tunnel --url http://127.0.0.1:8080
```

Then set this tunnel URL in script properties via Apps Script editor:

```javascript
setBackendApiBaseUrl('https://<your-random-subdomain>.trycloudflare.com/api')
```

Or from sheet `config!B5` (if you store URL there):

```javascript
setBackendApiBaseUrlFromConfig()
```
