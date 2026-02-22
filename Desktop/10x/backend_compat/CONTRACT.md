# Backend Compatibility Contract (1:1 target)

This document captures the API contract that the current Apps Script expects.

## Base URL

- Legacy base: `https://mp.btlz-api.ru/api`
- For your own backend, keep the same suffix style and expose:
  - `POST /api/ss/datasets/data`
  - `POST /api/ss/datasets/upload`
  - `POST /api/ss/datasets/update`
  - `POST /api/ss/wb/token/get`

The included server supports both `/api/...` and `/...` paths.

## Required payloads

### 1) Get dataset rows

`POST /api/ss/datasets/data`

Request:

```json
{
  "spreadsheet_id": "<google_sheet_id>",
  "dataset": {
    "name": "<dataset_name>",
    "values": {}
  }
}
```

Notes:
- Some script calls send `ssId` instead of `spreadsheet_id`.
- This server accepts both keys.

Response expected by script:
- Usually raw array (`[]` or `[{}, ...]`).
- For unsupported dataset names, do not fail hard in production: return empty array.

### 2) Upload/write dataset rows

`POST /api/ss/datasets/upload`

Request:

```json
{
  "spreadsheet_id": "<google_sheet_id>",
  "dataset": {
    "name": "<dataset_name>",
    "values": {}
  }
}
```

Response expected by script:
- Numeric count for action-plan and plan-save operations.

### 3) Trigger backend update

`POST /api/ss/datasets/update`

Request:

```json
{
  "spreadsheet_id": "<google_sheet_id>",
  "dataset": {
    "name": "<dataset_name>",
    "values": {}
  }
}
```

Response expected by script:
- Object with `message` is enough for menu notifications.

### 4) Resolve WB tokens for sheet

`POST /api/ss/wb/token/get`

Request:

```json
{
  "spreadsheet_id": "<google_sheet_id>"
}
```

Response:

```json
{
  "result": [
    {
      "token": "<wb_jwt>",
      "sid": "<supplier_id>",
      "analytics": true
    }
  ]
}
```

## Dataset names currently referenced

Data (`/ss/datasets/data`):
- `wbCardsData_v1`
- `wb10xSalesFinReportTotal_v1`
- `wb10xMain_planMonth_v1`
- `wbJamClusters_v1`
- `wb10xSalesReport_v1`
- `wb10xSalesReport_v2`
- `wb10xAdvNormqueryStatsByDatesNmIdsAdvertIds_v1`
- `wbActionPlan`
- `wb10xAnalyticsData_v1`

Current backend implementation note:
- `wbCardsData_v1` reads WB Content API cards.
- `wb10xMain_planMonth_v1`, `wb10xSalesFinReportTotal_v1`, `wb10xSalesReport_v1/v2`, `wb10xAnalyticsData_v1` use WB Statistics API for base metrics (orders/sales/stocks), then map into expected response schema.
- Advanced economic parts from original private backend are not fully reproduced yet (complex cost decomposition/business rules).

Upload (`/ss/datasets/upload`):
- `wb10xPlanMonthSave`
- `wb10xUnitSettingsSave` (daily UNIT snapshot for autonomous calculations)
- `wbActionPlanUpload`
- `wbActionPlanDelete`

Update (`/ss/datasets/update`):
- `wb10xSalesReport_v1`
- generic sheet refresh flow

## Extra action-based calls (not in BtlzApi)

The script also uses action-style calls to Google Apps Script web apps. In your own stack,
replace those with your backend endpoint (for example `POST /api/actions`) and route by action:

- `wb/analytics/nm-report/refresh_v2`
- `wb/adv/v1/upd/insert`
- `sheets/wb-plus/format-rules/reset`
- `processGroupCalculations` (from delivery calculator flow)

## Important integration detail

To be independent from the original owner, you also need to own Apps Script libraries (`common10x`, `BtlzApi`, `utilities10X`, etc.) and repoint `appsscript.json` library IDs to your copies.
