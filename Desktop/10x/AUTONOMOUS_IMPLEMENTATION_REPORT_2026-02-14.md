# Autonomous 1:1 Implementation Report
Date: 2026-02-14  
Project: `C:\Users\alien\Desktop\10x`

## 1) What Was Implemented

### 1.1 Historical snapshot storage (new DB schema)
Added persistent tables in `backend_compat/storage.py`:
- `search_keys`
- `daily_positions`
- `daily_adv`
- `daily_stocks`
- `daily_prices`
- `daily_funnel`

Also added upsert/read APIs:
- `upsert_search_keys()`, `list_search_keys()`
- `upsert_daily_positions()`, `get_daily_positions()`
- `upsert_daily_adv()`, `get_daily_adv()`
- `upsert_daily_stocks()`, `get_daily_stocks()`
- `upsert_daily_prices()`, `get_daily_prices()`
- `upsert_daily_funnel()`, `get_daily_funnel()`

File refs:
- `backend_compat/storage.py:82`
- `backend_compat/storage.py:485`

### 1.2 Collector (new autonomous parser pipeline)
Added `backend_compat/collector.py`:
- collects WB cards -> daily prices
- collects WB stocks -> daily stocks (with in-way split)
- collects WB adv updates -> daily adv spend by SKU and type
- collects WB funnel history -> open card/cart/wishlist
- builds/persists search keys
- parses and persists search positions

File refs:
- `backend_compat/collector.py:59`
- `backend_compat/collector.py:121`
- `backend_compat/collector.py:210`
- `backend_compat/collector.py:388`

### 1.3 API actions extended to real data collection workflows
In `backend_compat/server.py`:
- added `search/keys/upsert`
- added `search/positions/collect`
- improved `search/positions` to persist parsed results in DB when `ssId` passed
- `search/keys/refresh` now persists keys in DB
- added `collector/run` (and `wb/collector/run`) action

File refs:
- `backend_compat/server.py:415`
- `backend_compat/server.py:445`
- `backend_compat/server.py:642`
- `backend_compat/server.py:724`

### 1.4 Dataset builders connected to optional stored snapshots
In `backend_compat/datasets.py`:
- added feature flags:
  - `BTLZ_USE_STORED_DAILY_ADV`
  - `BTLZ_USE_STORED_DAILY_STOCKS`
  - `BTLZ_USE_STORED_DAILY_PRICES`
  - `BTLZ_USE_STORED_DAILY_FUNNEL`
- integrated stored daily sources into checklist/analytics builders
- added guard for long historical funnel windows to avoid unstable partial WB history in parity mode

Note: defaults are `0` (disabled) to avoid degrading historical parity when comparing against old static golden exports.

File refs:
- `backend_compat/datasets.py:97`
- `backend_compat/datasets.py:927`
- `backend_compat/datasets.py:1356`
- `backend_compat/datasets.py:1552`
- `backend_compat/datasets.py:3157`

### 1.5 Documentation updated
Updated `backend_compat/README.md`:
- new actions
- new env flags
- collector CLI/API usage

File refs:
- `backend_compat/README.md:36`
- `backend_compat/README.md:75`
- `backend_compat/README.md:96`

## 2) Validation Performed

### 2.1 Syntax/compile checks
Executed:
- `python -m py_compile backend_compat/server.py backend_compat/datasets.py backend_compat/collector.py backend_compat/wb_client.py backend_compat/storage.py`

Result: pass.

### 2.2 Collector dry runs
Executed:
- `python backend_compat/collector.py --spreadsheet-id <SHEET_ID> --date-from 2026-02-13 --date-to 2026-02-14 --no-positions`
- `python backend_compat/collector.py --spreadsheet-id <SHEET_ID> --date-from 2026-02-14 --date-to 2026-02-14 --no-funnel`
- `python backend_compat/collector.py --spreadsheet-id <SHEET_ID> --date-from 2025-11-01 --date-to 2026-02-14 --no-positions --no-funnel` (backfill)
- `python backend_compat/collector.py --spreadsheet-id <SHEET_ID> --date-from 2026-02-07 --date-to 2026-02-14 --no-positions` (funnel backfill window)

Observed insertions:
- prices: 113 rows
- stocks: 4028 rows
- adv: 1085 rows
- funnel: 864 rows
- search_keys: 4 rows
- positions: 4 rows

### 2.3 DB state after collection
Current counts in `backend_compat/data/btlz.db`:
- `daily_prices`: 113
- `daily_stocks`: 4028
- `daily_adv`: 1085
- `daily_funnel`: 864
- `search_keys`: 4
- `daily_positions`: 4

### 2.4 Parity diff run
Executed:
- `python backend_compat/checklist_cross_diff.py --spreadsheet-id <SHEET_ID> --csv tmp_gid_674172176.csv --out backend_compat/data/checklist_cross_autonomous_diff_report_after_stability_guard_20260214.json`

Result summary:
- `numeric_exact_ratio`: `0.768797`
- `missing_value_ratio`: `0.0`
- `numeric_mae`: `159.307456`
- `numeric_mape`: `10.602073`

Important context:
- this golden CSV is historical (`2025-11-01..2026-02-13`);
- WB source data (especially ads/returns attribution) is mutable post-factum;
- exact parity naturally drifts over time without same historical snapshot base that competitor had.

### 2.5 Parity diff with snapshots enabled
Executed with flags:
- `BTLZ_USE_STORED_DAILY_ADV=1`
- `BTLZ_USE_STORED_DAILY_STOCKS=1`
- `BTLZ_USE_STORED_DAILY_PRICES=1`
- `BTLZ_USE_STORED_DAILY_FUNNEL=1`

And report:
- `backend_compat/data/checklist_cross_autonomous_diff_report_with_snapshots_20260214.json`

Result:
- `numeric_exact_ratio`: `0.783509`
- `numeric_mae`: `127.901914`
- `numeric_mape`: `10.505725`

Net effect vs baseline (snapshots off):
- exact: `0.768797 -> 0.783509`
- MAE: `159.307456 -> 127.901914`

## 3) Why This Is Critical for Real 1:1

The main blocker for autonomous 1:1 was missing historical layers.  
This iteration adds the missing mechanism: **daily snapshot accumulation**.

Without this layer, backend recomputes history from mutable WB endpoints, which cannot reproduce an old frozen table 1:1.

With this layer enabled and running daily:
- future periods become reproducible;
- drift in `adv_sum`, `profit_*`, `stocks_*`, `avg_price_with_spp` is reduced by using captured day-state.

## 4) Immediate Next Execution Plan (Operational)

1. Run collector on schedule (cron/task scheduler), at least every 6-12 hours:
   - `python backend_compat/collector.py --spreadsheet-id <SHEET_ID> --date-from <today> --date-to <today>`
2. Enable stored snapshot usage in backend runtime:
   - `BTLZ_USE_STORED_DAILY_ADV=1`
   - `BTLZ_USE_STORED_DAILY_STOCKS=1`
   - `BTLZ_USE_STORED_DAILY_PRICES=1`
   - `BTLZ_USE_STORED_DAILY_FUNNEL=1`
3. Seed `search_keys` per SKU (manual or via `search/keys/refresh`) and run `search/positions/collect`.
4. Re-run parity diff on fresh exported tabs after 3-7 days of snapshot accumulation.
