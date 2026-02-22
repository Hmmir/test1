import argparse
import csv
import json
import math
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def _to_int(value: Any) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, bool):
        return float(int(value))
    if isinstance(value, (int, float)):
        try:
            if math.isnan(float(value)):
                return None
        except Exception:
            pass
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


def _load_sheet_cross_csv(path: Path) -> Tuple[List[str], List[int], List[str], Dict[Tuple[int, str, str], Any]]:
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        rows = list(reader)

    if not rows or len(rows[0]) < 4:
        raise RuntimeError("invalid checklist_cross csv: missing header")

    header = rows[0]
    date_cols = [str(x).strip() for x in header[3:] if str(x).strip()]
    if not date_cols:
        raise RuntimeError("invalid checklist_cross csv: no date columns")

    nm_ids: List[int] = []
    keys: List[str] = []
    values: Dict[Tuple[int, str, str], Any] = {}

    seen_nm = set()
    seen_key = set()
    for row in rows[1:]:
        if not row or not str(row[0]).strip():
            continue
        nm_id = _to_int(row[0])
        key = str(row[1] or "").strip()
        if not nm_id or not key:
            continue
        if nm_id not in seen_nm:
            seen_nm.add(nm_id)
            nm_ids.append(nm_id)
        if key not in seen_key:
            seen_key.add(key)
            keys.append(key)
        for idx, day in enumerate(date_cols, start=3):
            raw = row[idx] if idx < len(row) else ""
            values[(nm_id, key, day)] = raw

    nm_ids.sort()
    keys.sort()
    return date_cols, nm_ids, keys, values


def _index_checklist_rows(rows: List[Dict[str, Any]]) -> Dict[Tuple[int, str], Dict[str, Any]]:
    out: Dict[Tuple[int, str], Dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        nm_id = _to_int(row.get("nm_id"))
        day = str(row.get("date") or "")[:10]
        if not nm_id or not day:
            continue
        out[(nm_id, day)] = row
    return out


def _build_autonomous_cross_values(
    spreadsheet_id: str,
    nm_ids: List[int],
    date_from: str,
    date_to: str,
    db_path: str,
) -> Dict[Tuple[int, str, str], Any]:
    import sys

    # Local imports assume repo style (no package layout).
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from datasets import handle_checklist  # type: ignore
    from storage import Storage  # type: ignore

    storage = Storage(db_path)
    rows = handle_checklist(storage, spreadsheet_id, nm_ids, date_from, date_to=date_to)
    idx = _index_checklist_rows(rows)

    out: Dict[Tuple[int, str, str], Any] = {}
    # Extract only the requested date window (CSV header range).
    for nm_id in nm_ids:
        for day in _date_range(date_from, date_to):
            row = idx.get((nm_id, day))
            if row is None:
                continue
            for key, value in row.items():
                # Only keys relevant to the cross diff will be read by the caller.
                out[(nm_id, str(key), day)] = value
    return out


def _date_range(date_from: str, date_to: str) -> List[str]:
    try:
        start = datetime.strptime(date_from[:10], "%Y-%m-%d")
        end = datetime.strptime(date_to[:10], "%Y-%m-%d")
    except ValueError:
        return []
    if end < start:
        return []
    out: List[str] = []
    cur = start
    while cur <= end:
        out.append(cur.strftime("%Y-%m-%d"))
        cur = cur + timedelta(days=1)
    return out


def _compare(
    date_cols: List[str],
    nm_ids: List[int],
    keys: List[str],
    sheet_values: Dict[Tuple[int, str, str], Any],
    auto_values: Dict[Tuple[int, str, str], Any],
) -> Dict[str, Any]:
    total_cells = len(nm_ids) * len(keys) * len(date_cols)

    # (nm_id, day) presence check from any key.
    present_nm_day = set()
    for nm_id in nm_ids:
        for day in date_cols:
            if (nm_id, keys[0], day) in auto_values or any(
                (nm_id, key, day) in auto_values for key in keys
            ):
                present_nm_day.add((nm_id, day))

    key_rows: List[Dict[str, Any]] = []

    numeric_compared_total = 0
    numeric_exact_total = 0
    numeric_abs_sum_total = 0.0
    numeric_rel_sum_total = 0.0
    numeric_rel_count_total = 0

    missing_cells_total = 0
    sheet_nonzero_missing_cells_total = 0

    for key in keys:
        compared = 0
        exact = 0
        abs_sum = 0.0
        rel_sum = 0.0
        rel_count = 0
        missing_cells = 0
        sheet_nonzero_missing_cells = 0
        string_compared = 0
        string_exact = 0

        for nm_id in nm_ids:
            for day in date_cols:
                sv_raw = sheet_values.get((nm_id, key, day))
                av_raw = auto_values.get((nm_id, key, day))

                # Missing detection (structural parity).
                if av_raw is None:
                    missing_cells += 1
                    sv_num = _to_float(sv_raw)
                    if sv_num is not None and abs(sv_num) > 1e-9:
                        sheet_nonzero_missing_cells += 1
                    continue

                sv = _to_float(sv_raw)
                av = _to_float(av_raw)
                if sv is None or av is None:
                    string_compared += 1
                    if str(sv_raw or "") == str(av_raw or ""):
                        string_exact += 1
                    continue

                compared += 1
                delta = av - sv
                abs_delta = abs(delta)
                abs_sum += abs_delta
                tol = max(abs(sv) * 0.01, 0.01)
                if abs_delta <= tol:
                    exact += 1
                if abs(sv) > 1e-9:
                    rel_sum += abs_delta / abs(sv)
                    rel_count += 1

        total_key_cells = len(nm_ids) * len(date_cols)
        present_ratio = (
            (len(present_nm_day) / float(len(nm_ids) * len(date_cols)))
            if nm_ids and date_cols
            else 0.0
        )
        key_rows.append(
            {
                "key": key,
                "total_cells": total_key_cells,
                "present_nm_date_ratio": round(present_ratio, 6),
                "missing_value_ratio": round(missing_cells / float(total_key_cells), 6)
                if total_key_cells
                else 0.0,
                "sheet_nonzero_missing_cells": sheet_nonzero_missing_cells,
                "numeric_compared": compared,
                "numeric_exact_ratio": round(exact / float(compared), 6) if compared else 0.0,
                "mae": round(abs_sum / float(compared), 6) if compared else 0.0,
                "mape": round(rel_sum / float(rel_count), 6) if rel_count else 0.0,
                "string_compared": string_compared,
                "string_exact_ratio": round(string_exact / float(string_compared), 6)
                if string_compared
                else None,
            }
        )

        numeric_compared_total += compared
        numeric_exact_total += exact
        numeric_abs_sum_total += abs_sum
        numeric_rel_sum_total += rel_sum
        numeric_rel_count_total += rel_count
        missing_cells_total += missing_cells
        sheet_nonzero_missing_cells_total += sheet_nonzero_missing_cells

    summary = {
        "sheet": "checklist_cross",
        "date_from": date_cols[0] if date_cols else "",
        "date_to": date_cols[-1] if date_cols else "",
        "nm_ids_count": len(nm_ids),
        "keys_count": len(keys),
        "total_cells": total_cells,
        "autonomous_rows": len(present_nm_day),
        "autonomous_nm_date_keys": len(present_nm_day),
        "missing_value_ratio": round(missing_cells_total / float(total_cells), 6)
        if total_cells
        else 0.0,
        "sheet_nonzero_missing_ratio": round(
            sheet_nonzero_missing_cells_total / float(total_cells), 6
        )
        if total_cells
        else 0.0,
        "numeric_compared": numeric_compared_total,
        "numeric_exact_ratio": round(numeric_exact_total / float(numeric_compared_total), 6)
        if numeric_compared_total
        else 0.0,
        "numeric_mae": round(numeric_abs_sum_total / float(numeric_compared_total), 6)
        if numeric_compared_total
        else 0.0,
        "numeric_mape": round(numeric_rel_sum_total / float(numeric_rel_count_total), 6)
        if numeric_rel_count_total
        else 0.0,
        "string_compared": sum(r["string_compared"] for r in key_rows),
        "string_exact_ratio": None,
    }

    top_mae = sorted(key_rows, key=lambda r: r["mae"], reverse=True)[:10]
    top_mape = sorted(key_rows, key=lambda r: r["mape"], reverse=True)[:10]
    top_missing = sorted(
        key_rows, key=lambda r: (r["sheet_nonzero_missing_cells"], r["missing_value_ratio"]), reverse=True
    )[:10]

    return {
        "summary": summary,
        "top_mae": top_mae,
        "top_mape": top_mape,
        "top_missing": top_missing,
        "key_rows": key_rows,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True, help="Path to exported checklist_cross CSV")
    ap.add_argument("--spreadsheet-id", required=True, help="Spreadsheet ID for token lookup")
    ap.add_argument("--gid", default="", help="Optional sheet gid (for report metadata)")
    ap.add_argument(
        "--db-path",
        default="",
        help="Optional DB path. Default: backend_compat/data/btlz.db",
    )
    ap.add_argument(
        "--output",
        default="",
        help="Output JSON path. Default: backend_compat/data/checklist_cross_autonomous_diff_report_<ts>.json",
    )
    args = ap.parse_args()

    csv_path = Path(args.csv)
    date_cols, nm_ids, keys, sheet_values = _load_sheet_cross_csv(csv_path)
    date_from, date_to = date_cols[0], date_cols[-1]

    auto_values = _build_autonomous_cross_values(
        spreadsheet_id=str(args.spreadsheet_id),
        nm_ids=nm_ids,
        date_from=date_from,
        date_to=date_to,
        db_path=str(
            args.db_path
            or (Path(__file__).resolve().parent / "data" / "btlz.db")
        ),
    )

    report = _compare(date_cols, nm_ids, keys, sheet_values, auto_values)
    report["summary"]["gid"] = str(args.gid or "")

    out_path = Path(args.output) if args.output else None
    if out_path is None:
        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        out_path = Path(__file__).resolve().parent / "data" / f"checklist_cross_autonomous_diff_report_{ts}.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(str(out_path))


if __name__ == "__main__":
    main()
