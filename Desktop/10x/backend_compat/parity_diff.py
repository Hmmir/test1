import argparse
import json
import math
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import urllib.error
import urllib.request

from datasets import handle_data
from storage import Storage
from wb import token_sid
from urllib_open import open_url


def _to_int(value: Any) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def _to_float(value: Any) -> Optional[float]:
    if isinstance(value, bool):
        return float(int(value))
    if isinstance(value, (int, float)):
        if math.isnan(float(value)):
            return None
        return float(value)
    if isinstance(value, str):
        text = value.strip().replace(" ", "").replace(",", ".")
        if not text:
            return None
        try:
            return float(text)
        except ValueError:
            return None
    return None


def _normalize_base(url: str) -> str:
    return url.rstrip("/")


def _post_json(
    base_url: str, path: str, payload: Dict[str, Any], timeout: int = 120
) -> Any:
    req = urllib.request.Request(
        _normalize_base(base_url) + path,
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with open_url(req, timeout=timeout) as resp:
            raw = resp.read()
            if not raw:
                return {}
            return json.loads(raw.decode("utf-8"))
    except urllib.error.HTTPError as exc:
        details = exc.read().decode("utf-8", "replace")
        raise RuntimeError(f"HTTP {exc.code} {path}: {details}")


def _extract_rows(resp: Any) -> List[Dict[str, Any]]:
    if isinstance(resp, list):
        return [row for row in resp if isinstance(row, dict)]
    if isinstance(resp, dict):
        result = resp.get("result")
        if isinstance(result, list):
            return [row for row in result if isinstance(row, dict)]
    return []


def _fetch_original_token(
    orig_base: str, source_sheet_id: str
) -> Tuple[str, Optional[str]]:
    resp = _post_json(
        orig_base,
        "/ss/wb/token/get",
        {"spreadsheet_id": source_sheet_id},
    )
    rows = []
    if isinstance(resp, dict) and isinstance(resp.get("result"), list):
        rows = [r for r in resp["result"] if isinstance(r, dict)]
    if not rows:
        raise RuntimeError("No tokens returned by original backend")

    best = rows[0]
    for row in rows:
        if row.get("is_valid") is True:
            best = row
            break

    token = str(best.get("token") or "").strip()
    if not token:
        raise RuntimeError("Original backend token payload does not include token")

    sid_raw = best.get("sid")
    sid = str(sid_raw).strip() if sid_raw else None
    if not sid:
        sid = token_sid(token)
    return token, sid


def _fetch_sample_nm_ids(
    orig_base: str,
    source_sheet_id: str,
    sample_size: int,
    sid: Optional[str],
) -> List[int]:
    values: Dict[str, Any] = {}
    if sid:
        values["sid"] = sid
    payload = {
        "spreadsheet_id": source_sheet_id,
        "dataset": {
            "name": "wbCardsData_v1",
            "values": values,
        },
    }
    rows = _extract_rows(_post_json(orig_base, "/ss/datasets/data", payload))
    out: List[int] = []
    seen = set()
    for row in rows:
        nm_id = _to_int(row.get("nm_id") or row.get("nmId") or row.get("nmID"))
        if not nm_id or nm_id in seen:
            continue
        seen.add(nm_id)
        out.append(nm_id)
        if len(out) >= sample_size:
            break
    if not out:
        raise RuntimeError("Failed to build sample nm_ids from wbCardsData_v1")
    return out


def _index_by_nm(rows: List[Dict[str, Any]]) -> Dict[int, Dict[str, Any]]:
    out: Dict[int, Dict[str, Any]] = {}
    for row in rows:
        nm_id = _to_int(row.get("nm_id") or row.get("nmId") or row.get("nmID"))
        if nm_id:
            out[nm_id] = row
    return out


def _filter_rows_by_nm_ids(
    rows: List[Dict[str, Any]], nm_ids: List[int]
) -> List[Dict[str, Any]]:
    if not nm_ids:
        return rows
    selected = set(nm_ids)
    return [
        row
        for row in rows
        if _to_int(row.get("nm_id") or row.get("nmId") or row.get("nmID")) in selected
    ]


def _field_list(
    orig_rows: List[Dict[str, Any]], local_rows: List[Dict[str, Any]]
) -> List[str]:
    keys = set()
    for row in orig_rows + local_rows:
        keys.update(row.keys())
    keys.discard("nm_id")
    keys.discard("nmId")
    keys.discard("nmID")
    return sorted(keys)


def _compare_rows(
    dataset: str,
    orig_rows: List[Dict[str, Any]],
    local_rows: List[Dict[str, Any]],
    focus_fields: List[str],
) -> Dict[str, Any]:
    orig_idx = _index_by_nm(orig_rows)
    local_idx = _index_by_nm(local_rows)

    orig_ids = set(orig_idx.keys())
    local_ids = set(local_idx.keys())
    common_ids = sorted(orig_ids & local_ids)

    fields = _field_list(orig_rows, local_rows)
    by_field: Dict[str, Dict[str, Any]] = {}

    for field in fields:
        compared = 0
        abs_sum = 0.0
        abs_max = 0.0
        rel_sum = 0.0
        rel_count = 0
        exact_count = 0
        max_example: Optional[Dict[str, Any]] = None

        for nm_id in common_ids:
            o = _to_float(orig_idx[nm_id].get(field))
            loc = _to_float(local_idx[nm_id].get(field))
            if o is None or loc is None:
                continue

            compared += 1
            delta = loc - o
            abs_delta = abs(delta)
            abs_sum += abs_delta
            if abs_delta > abs_max:
                abs_max = abs_delta
                max_example = {
                    "nm_id": nm_id,
                    "orig": o,
                    "local": loc,
                    "delta": delta,
                }

            if abs(o) > 1e-9:
                rel_sum += abs_delta / abs(o)
                rel_count += 1

            tolerance = max(abs(o) * 0.01, 0.01)
            if abs_delta <= tolerance:
                exact_count += 1

        if compared == 0:
            continue

        by_field[field] = {
            "compared": compared,
            "mae": round(abs_sum / compared, 6),
            "mape": round((rel_sum / rel_count) if rel_count else 0.0, 6),
            "max_abs": round(abs_max, 6),
            "exact_ratio": round(exact_count / compared, 6),
            "max_example": max_example,
        }

    focus_rows = []
    for field in focus_fields:
        item = by_field.get(field)
        if item:
            focus_rows.append({"field": field, **item})

    top_drift = sorted(
        [{"field": k, **v} for k, v in by_field.items()],
        key=lambda x: (x["mae"], x["max_abs"]),
        reverse=True,
    )[:25]

    return {
        "dataset": dataset,
        "counts": {
            "orig_rows": len(orig_rows),
            "local_rows": len(local_rows),
            "orig_nm_ids": len(orig_ids),
            "local_nm_ids": len(local_ids),
            "common_nm_ids": len(common_ids),
            "missing_in_local": sorted(orig_ids - local_ids),
            "extra_in_local": sorted(local_ids - orig_ids),
        },
        "focus": focus_rows,
        "top_drift": top_drift,
        "fields": by_field,
    }


def _print_dataset_report(report: Dict[str, Any]) -> None:
    dataset = report["dataset"]
    counts = report["counts"]
    print(f"\n=== {dataset} ===")
    print(
        "rows/orig-local-common:",
        counts["orig_rows"],
        counts["local_rows"],
        counts["common_nm_ids"],
    )

    focus = report.get("focus", [])
    if focus:
        print("focus fields:")
        for row in focus:
            print(
                f"  - {row['field']}: mae={row['mae']:.6f} "
                f"mape={row['mape']:.6f} exact={row['exact_ratio']:.3f}"
            )

    print("top drift fields:")
    for row in report.get("top_drift", [])[:10]:
        print(
            f"  - {row['field']}: mae={row['mae']:.6f} "
            f"max={row['max_abs']:.6f} exact={row['exact_ratio']:.3f}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Original-vs-local parity diff runner")
    parser.add_argument(
        "--orig-base",
        default="https://mp.btlz-api.ru/api",
        help="Original backend base URL",
    )
    parser.add_argument(
        "--source-sheet-id",
        default="1jJ16n1F4CzS0Gm9tDZGp_h8pxRDETmFu_73gm2qSwV8",
        help="Spreadsheet id registered in original backend",
    )
    parser.add_argument(
        "--target-sheet-id",
        default="1pZFdu2jxBXJFnLhQW1xHU0qT-pH4IKtK6W9otr_hHsw",
        help="Spreadsheet id used in local backend storage",
    )
    parser.add_argument(
        "--db-path",
        default=str(Path(__file__).resolve().parent / "data" / "btlz.db"),
        help="Local sqlite db path",
    )
    parser.add_argument("--date-from", default="2026-01-01")
    parser.add_argument("--date-to", default="2026-01-31")
    parser.add_argument("--sample-size", type=int, default=60)
    parser.add_argument(
        "--datasets",
        default="wb10xMain_planMonth_v1,wb10xSalesFinReportTotal_v1",
        help="Comma-separated dataset names",
    )
    parser.add_argument(
        "--nm-ids",
        default="",
        help="Optional comma-separated nm_ids override",
    )
    parser.add_argument(
        "--report-file",
        default="",
        help="Optional output report path (json)",
    )
    args = parser.parse_args()

    token, sid = _fetch_original_token(args.orig_base, args.source_sheet_id)

    storage = Storage(args.db_path)
    storage.register_spreadsheet(args.target_sheet_id)
    storage.add_wb_token(args.target_sheet_id, token, sid)

    nm_ids: List[int] = []
    if args.nm_ids.strip():
        for part in args.nm_ids.split(","):
            nm_id = _to_int(part)
            if nm_id:
                nm_ids.append(nm_id)
    if not nm_ids:
        nm_ids = _fetch_sample_nm_ids(
            args.orig_base,
            args.source_sheet_id,
            max(1, args.sample_size),
            sid,
        )

    dataset_names = [d.strip() for d in args.datasets.split(",") if d.strip()]
    focus_fields = [
        "buyout_count",
        "buyout_percent",
        "tax",
        "tax_perc",
        "promos_sum",
        "external_costs",
        "additional_costs",
        "cancel_count",
        "cancel_sum_rub",
    ]

    reports = []
    for dataset in dataset_names:
        values: Dict[str, Any] = {
            "date_from": args.date_from,
            "date_to": args.date_to,
        }

        if dataset == "wb10xSalesFinReportTotal_v1":
            values["sid"] = sid or ""
            values["forced"] = True
        else:
            values["nm_ids"] = nm_ids
            if sid:
                values["sid"] = sid

        payload = {
            "spreadsheet_id": args.source_sheet_id,
            "dataset": {"name": dataset, "values": values},
        }
        orig_rows = _extract_rows(
            _post_json(args.orig_base, "/ss/datasets/data", payload)
        )
        orig_rows = _filter_rows_by_nm_ids(orig_rows, nm_ids)
        local_rows = handle_data(storage, args.target_sheet_id, dataset, values)
        if not isinstance(local_rows, list):
            local_rows = []
        local_rows = _filter_rows_by_nm_ids(local_rows, nm_ids)

        report = _compare_rows(dataset, orig_rows, local_rows, focus_fields)
        reports.append(report)
        _print_dataset_report(report)

    final = {
        "timestamp_utc": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "source_sheet_id": args.source_sheet_id,
        "target_sheet_id": args.target_sheet_id,
        "date_from": args.date_from,
        "date_to": args.date_to,
        "sample_nm_ids": nm_ids,
        "sample_size": len(nm_ids),
        "datasets": reports,
    }

    report_path = args.report_file.strip()
    if not report_path:
        stamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        report_path = str(
            Path(__file__).resolve().parent / "data" / f"parity_report_{stamp}.json"
        )

    report_file = Path(report_path)
    report_file.parent.mkdir(parents=True, exist_ok=True)
    report_file.write_text(
        json.dumps(final, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(f"\nSaved report: {report_file}")


if __name__ == "__main__":
    main()
