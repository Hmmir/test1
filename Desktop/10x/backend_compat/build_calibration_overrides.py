import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import urllib.error
import urllib.request

from wb import token_sid
from urllib_open import open_url

PLAN_MONTH_FIELDS = [
    "avg_price",
    "days_in_stock",
    "checklist_orders_sum",
    "checklist_orders_count",
    "checklist_buyouts_sum",
    "checklist_buyouts_count",
    "orders_ext_perc",
    "adv_sum_auto_search",
    "stocks_fbo",
    "stocks_fbs",
    "buyout_percent",
    "sebes_rub",
    "markirovka_rub",
    "perc_mp",
    "delivery_mp_with_buyout_rub",
    "hranenie_rub",
    "acquiring_perc",
    "tax_total_perc",
    "additional_costs",
    "priemka_rub",
    "spp",
]


def _to_int(value: Any) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def _to_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _post_json(
    base_url: str, path: str, payload: Dict[str, Any], timeout: int = 120
) -> Any:
    req = urllib.request.Request(
        base_url.rstrip("/") + path,
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

    rows = _extract_rows(
        _post_json(
            orig_base,
            "/ss/datasets/data",
            {
                "spreadsheet_id": source_sheet_id,
                "dataset": {
                    "name": "wbCardsData_v1",
                    "values": values,
                },
            },
        )
    )

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
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Build nm_id calibration overrides")
    parser.add_argument(
        "--orig-base",
        default="https://mp.btlz-api.ru/api",
        help="Original backend base URL",
    )
    parser.add_argument(
        "--source-sheet-id",
        default="1jJ16n1F4CzS0Gm9tDZGp_h8pxRDETmFu_73gm2qSwV8",
        help="Spreadsheet id in original backend",
    )
    parser.add_argument("--date-from", default="2026-01-01")
    parser.add_argument("--date-to", default="2026-01-31")
    parser.add_argument("--sample-size", type=int, default=200)
    parser.add_argument(
        "--nm-ids",
        default="",
        help="Optional comma-separated nm_ids",
    )
    parser.add_argument(
        "--output",
        default=str(
            Path(__file__).resolve().parent / "data" / "calibration_overrides.json"
        ),
        help="Output calibration file path",
    )
    args = parser.parse_args()

    token, sid = _fetch_original_token(args.orig_base, args.source_sheet_id)

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

    values = {
        "date_from": args.date_from,
        "date_to": args.date_to,
        "sid": sid or "",
        "forced": True,
    }
    rows = _extract_rows(
        _post_json(
            args.orig_base,
            "/ss/datasets/data",
            {
                "spreadsheet_id": args.source_sheet_id,
                "dataset": {
                    "name": "wb10xSalesFinReportTotal_v1",
                    "values": values,
                },
            },
        )
    )

    selected = set(nm_ids)
    output_overrides: Dict[str, Dict[str, Any]] = {}

    for row in rows:
        nm_id = _to_int(row.get("nm_id") or row.get("nmId") or row.get("nmID"))
        if not nm_id or (selected and nm_id not in selected):
            continue

        buyout_count = max(_to_int(row.get("buyout_count")), 0)
        income_sum = max(_to_float(row.get("income_sum_rub")), 0.0)
        sebes_total = max(_to_float(row.get("sebes_rub")), 0.0)
        adv_sum = max(_to_float(row.get("adv_sum")), 0.0)
        tax_total = _to_float(row.get("tax"))
        warehouse_price = max(_to_float(row.get("warehouse_price")), 0.0)
        additional_payment_total = max(
            _to_float(row.get("additional_payment_total")), 0.0
        )
        total_wb_comission = _to_float(row.get("total_wb_comission"))
        tax_perc = max(_to_float(row.get("tax_perc")), 0.0)
        perc_mp = max(_to_float(row.get("commission_wb_perc")), 0.0)

        sebes_unit = sebes_total / float(buyout_count) if buyout_count > 0 else 0.0
        adv_per_buyout = adv_sum / float(buyout_count) if buyout_count > 0 else 0.0
        warehouse_per_buyout = (
            warehouse_price / float(buyout_count) if buyout_count > 0 else 0.0
        )
        add_payment_per_buyout = (
            additional_payment_total / float(buyout_count) if buyout_count > 0 else 0.0
        )
        adv_income_ratio = adv_sum / income_sum if income_sum > 0 else 0.0
        warehouse_income_ratio = warehouse_price / income_sum if income_sum > 0 else 0.0
        add_payment_income_ratio = (
            additional_payment_total / income_sum if income_sum > 0 else 0.0
        )
        total_wb_income_ratio = (
            total_wb_comission / income_sum if income_sum > 0 else 0.0
        )

        output_overrides[str(nm_id)] = {
            "sebes_rub_unit": round(sebes_unit, 6),
            "adv_sum_total": round(adv_sum, 6),
            "adv_sum_per_buyout": round(adv_per_buyout, 6),
            "adv_sum_income_ratio": round(adv_income_ratio, 6),
            "tax_total": round(tax_total, 6),
            "warehouse_price_total": round(warehouse_price, 6),
            "warehouse_price_per_buyout": round(warehouse_per_buyout, 6),
            "warehouse_price_income_ratio": round(warehouse_income_ratio, 6),
            "additional_payment_total": round(additional_payment_total, 6),
            "additional_payment_per_buyout": round(add_payment_per_buyout, 6),
            "additional_payment_income_ratio": round(add_payment_income_ratio, 6),
            "total_wb_comission_total": round(total_wb_comission, 6),
            "total_wb_comission_income_ratio": round(total_wb_income_ratio, 6),
            "tax_rate_hint": round(tax_perc, 6),
            "perc_mp_hint": round(perc_mp, 6),
            "source_buyout_count": buyout_count,
            "source_income_sum_rub": round(income_sum, 2),
        }

    plan_values: Dict[str, Any] = {
        "date_from": args.date_from,
        "date_to": args.date_to,
        "nm_ids": nm_ids,
    }
    if sid:
        plan_values["sid"] = sid

    plan_rows = _extract_rows(
        _post_json(
            args.orig_base,
            "/ss/datasets/data",
            {
                "spreadsheet_id": args.source_sheet_id,
                "dataset": {
                    "name": "wb10xMain_planMonth_v1",
                    "values": plan_values,
                },
            },
        )
    )

    matched_plan_rows = 0
    for row in plan_rows:
        nm_id = _to_int(row.get("nm_id") or row.get("nmId") or row.get("nmID"))
        if not nm_id or (selected and nm_id not in selected):
            continue

        plan_row: Dict[str, Any] = {}
        for field in PLAN_MONTH_FIELDS:
            if field in row:
                plan_row[field] = row.get(field)

        if not plan_row:
            continue

        item = output_overrides.setdefault(str(nm_id), {})
        item["plan_row"] = plan_row
        matched_plan_rows += 1

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    payload = {
        "meta": {
            "generated_at_utc": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "source_sheet_id": args.source_sheet_id,
            "date_from": args.date_from,
            "date_to": args.date_to,
            "sample_size": len(nm_ids),
            "matched_rows": len(output_overrides),
            "matched_plan_rows": matched_plan_rows,
        },
        "overrides": output_overrides,
    }

    out_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"Saved calibration overrides: {out_path}")
    print(f"Matched nm_ids: {len(output_overrides)}")


if __name__ == "__main__":
    main()
