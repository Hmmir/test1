import json
import os
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Optional

from urllib_open import open_url


UPSTREAM_BASE = str(
    os.environ.get("BTLZ_UPSTREAM_BASE", "https://mp.btlz-api.ru/api")
).strip()
UPSTREAM_SOURCE_SHEET_ID = str(
    os.environ.get(
        "BTLZ_UPSTREAM_SOURCE_SHEET_ID",
        "1jJ16n1F4CzS0Gm9tDZGp_h8pxRDETmFu_73gm2qSwV8",
    )
).strip()
UPSTREAM_TIMEOUT = int(os.environ.get("BTLZ_UPSTREAM_TIMEOUT", "60"))
UPSTREAM_ENABLED = str(os.environ.get("BTLZ_UPSTREAM_ENABLED", "0")).strip() in {
    "1",
    "true",
    "True",
}


def _post_json(path: str, payload: Dict[str, Any], timeout: int = UPSTREAM_TIMEOUT) -> Any:
    if not UPSTREAM_BASE:
        raise RuntimeError("upstream base is not configured")
    req = urllib.request.Request(
        UPSTREAM_BASE.rstrip("/") + path,
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with open_url(req, timeout=timeout) as resp:
        raw = resp.read()
    if not raw:
        return {}
    return json.loads(raw.decode("utf-8"))


def _get_json(path: str, timeout: int = UPSTREAM_TIMEOUT) -> Any:
    if not UPSTREAM_BASE:
        raise RuntimeError("upstream base is not configured")
    req = urllib.request.Request(
        UPSTREAM_BASE.rstrip("/") + path,
        method="GET",
    )
    with open_url(req, timeout=timeout) as resp:
        raw = resp.read()
    if not raw:
        return []
    return json.loads(raw.decode("utf-8"))


def _extract_rows(payload: Any) -> Optional[List[Dict[str, Any]]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        result = payload.get("result")
        if isinstance(result, list):
            return [item for item in result if isinstance(item, dict)]
    return None


def fetch_dataset_data(
    spreadsheet_id: str, dataset_name: str, values: Dict[str, Any]
) -> Optional[List[Dict[str, Any]]]:
    if not UPSTREAM_ENABLED or not UPSTREAM_BASE:
        return None

    payload = {
        "spreadsheet_id": spreadsheet_id,
        "dataset": {"name": dataset_name, "values": values},
    }
    source_ids = [spreadsheet_id]
    if UPSTREAM_SOURCE_SHEET_ID and UPSTREAM_SOURCE_SHEET_ID not in source_ids:
        source_ids.append(UPSTREAM_SOURCE_SHEET_ID)

    for source_sheet_id in source_ids:
        payload["spreadsheet_id"] = source_sheet_id
        try:
            result = _post_json("/ss/datasets/data", payload)
        except Exception:
            continue
        rows = _extract_rows(result)
        if rows is not None:
            return rows

    return None


def fetch_checklist(
    spreadsheet_id: str, nm_ids: List[int], date_from: str
) -> Optional[List[Dict[str, Any]]]:
    if not UPSTREAM_ENABLED or not UPSTREAM_BASE:
        return None

    source_ids = [spreadsheet_id]
    if UPSTREAM_SOURCE_SHEET_ID and UPSTREAM_SOURCE_SHEET_ID not in source_ids:
        source_ids.append(UPSTREAM_SOURCE_SHEET_ID)

    nm_ids_param = ",".join(str(item) for item in nm_ids if item)
    query = urllib.parse.urlencode(
        {
            "nm_ids": nm_ids_param,
            "date_from": date_from,
        }
    )

    for source_sheet_id in source_ids:
        path = f"/ss/{source_sheet_id}/dataset/wb/checklist?{query}"
        try:
            data = _get_json(path)
        except Exception:
            continue
        if isinstance(data, list):
            return [row for row in data if isinstance(row, dict)]
    return None
