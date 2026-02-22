import argparse
import bisect
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple

from storage import Storage
from wb_client import (
    build_adv_fullstats_daily_nm_spend,
    build_adv_normquery_rows,
    build_search_items_from_adv_rows,
    campaign_bucket,
    fetch_advert_items,
    fetch_adv_fullstats,
    fetch_adv_upd,
    fetch_cards,
    fetch_detail_history_report_csv,
    fetch_normquery_daily_stats,
    fetch_orders,
    fetch_sales_funnel_history,
    fetch_search_positions,
    fetch_search_positions_multi,
    fetch_stocks,
    fetch_tariffs_box,
    fetch_tariffs_commission,
)

STOCKS_MODE = str(os.environ.get("BTLZ_STOCKS_MODE", "snapshot") or "").strip().lower()
STOCKS_SNAPSHOT_LOOKBACK_DAYS = int(
    os.environ.get("BTLZ_STOCKS_SNAPSHOT_LOOKBACK_DAYS", "3650")
)
DETAIL_HISTORY_ENABLED = str(
    os.environ.get("BTLZ_DETAIL_HISTORY_ENABLED", "1") or ""
).strip().lower() in {
    "1",
    "true",
    "yes",
}
ADV_DAILY_MONEY_SOURCE = (
    str(os.environ.get("BTLZ_ADV_DAILY_MONEY_SOURCE", "hybrid") or "").strip().lower()
)


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


def _parse_ymd(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if len(text) >= 10:
        return text[:10]
    return text


def _date_range(date_from: str, date_to: str) -> List[str]:
    try:
        start = datetime.strptime(str(date_from)[:10], "%Y-%m-%d")
        end = datetime.strptime(str(date_to)[:10], "%Y-%m-%d")
    except ValueError:
        return []
    if end < start:
        return []
    out: List[str] = []
    cur = start
    while cur <= end:
        out.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)
    return out


REGION_KEYS = [
    "central",
    "northwest",
    "south_caucasus",
    "volga",
    "fareast",
    "ural",
]


def _month_chunks(date_from: str, date_to: str) -> List[Tuple[str, str]]:
    try:
        start = datetime.strptime(str(date_from)[:10], "%Y-%m-%d")
        end = datetime.strptime(str(date_to)[:10], "%Y-%m-%d")
    except ValueError:
        return []
    if end < start:
        return []
    chunks: List[Tuple[str, str]] = []
    cur = start
    while cur <= end:
        next_month = (cur.replace(day=28) + timedelta(days=4)).replace(day=1)
        month_end = next_month - timedelta(days=1)
        chunk_end = month_end if month_end < end else end
        chunks.append((cur.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d")))
        cur = chunk_end + timedelta(days=1)
    return chunks


def _region_bucket(value: Any) -> str:
    text = str(value or "").strip().lower().replace("ё", "е")
    if not text:
        return ""
    if "централ" in text:
        return "central"
    if "северо-запад" in text:
        return "northwest"
    if "северо-кавказ" in text or "южн" in text:
        return "south_caucasus"
    if "приволж" in text:
        return "volga"
    if "дальневост" in text:
        return "fareast"
    if "урал" in text:
        return "ural"
    return ""


def _order_region(row: Dict[str, Any]) -> str:
    return _region_bucket(
        row.get("oblastOkrugName")
        or row.get("oblast_okrug_name")
        or row.get("federalDistrict")
        or row.get("federal_district")
        or row.get("district")
    )


def _build_localization_daily_rows(
    order_rows: List[Dict[str, Any]],
    nm_filter: Set[int],
) -> List[Dict[str, Any]]:
    agg: Dict[Tuple[int, str], Dict[str, Any]] = {}
    for item in order_rows:
        if not isinstance(item, dict):
            continue
        nm_id = _to_int(item.get("nmId") or item.get("nmID") or item.get("nm_id"))
        if not nm_id:
            continue
        if nm_filter and nm_id not in nm_filter:
            continue
        day = _parse_ymd(item.get("date") or item.get("lastChangeDate"))
        if not day:
            continue
        qty = max(_to_int(item.get("quantity")), 1)
        region = _order_region(item)
        key = (nm_id, day)
        row = agg.setdefault(
            key,
            {
                "date": day,
                "nm_id": nm_id,
                "orders_count_total": 0,
                "orders_count_local": 0,
                "localization_percent": 0.0,
                "source": "orders_stats_regions",
                "raw_regions": {},
            },
        )
        row["orders_count_total"] += qty
        if region:
            total_key = f"orders_count_total_{region}"
            local_key = f"orders_count_local_{region}"
            row[total_key] = row.get(total_key, 0) + qty
            row[local_key] = row.get(local_key, 0) + qty
            raw_regions_any = row.get("raw_regions")
            raw_regions: Dict[str, int] = (
                raw_regions_any if isinstance(raw_regions_any, dict) else {}
            )
            raw_regions[region] = int(raw_regions.get(region, 0) or 0) + qty
            row["raw_regions"] = raw_regions

    out: List[Dict[str, Any]] = []
    for row in agg.values():
        total_sum = int(row.get("orders_count_total") or 0)
        for region in REGION_KEYS:
            total_key = f"orders_count_total_{region}"
            local_key = f"orders_count_local_{region}"
            total_val = int(row.get(total_key) or 0)
            local_val = int(row.get(local_key) or 0)
            row[total_key] = total_val
            row[local_key] = local_val
        # For checklist parity we treat "local" as central-region local count.
        # Region-specific columns remain available for deeper diagnostics.
        row["orders_count_local"] = int(row.get("orders_count_local_central") or 0)
        if total_sum > 0:
            row["localization_percent"] = round(
                (float(row["orders_count_local"]) / float(total_sum)) * 100.0, 6
            )
        else:
            row["localization_percent"] = 0.0
        out.append(row)
    return out


def _build_stock_daily_rows(
    stock_rows: List[Dict[str, Any]],
    days: List[str],
    nm_filter: Set[int],
) -> List[Dict[str, Any]]:
    by_variant: Dict[
        Tuple[int, str, str, str, str], List[Tuple[str, int, int, int]]
    ] = {}
    for stock in stock_rows:
        nm_id = _to_int(stock.get("nmId") or stock.get("nmID"))
        if not nm_id:
            continue
        if nm_filter and nm_id not in nm_filter:
            continue
        dt = _parse_ymd(stock.get("lastChangeDate"))
        if not dt:
            continue
        variant_key = (
            nm_id,
            str(stock.get("warehouseName") or ""),
            str(stock.get("barcode") or ""),
            str(stock.get("techSize") or ""),
            str(stock.get("supplierArticle") or ""),
        )
        # WB `/stocks` exposes both `quantity` and `quantityFull`.
        # For parity with checklist-style dashboards use `quantity` (available stock).
        # `quantityFull` often includes non-sellable/reserved stock and drifts from sheet numbers.
        qty = max(_to_int(stock.get("quantity")), 0)
        in_way_to = max(_to_int(stock.get("inWayToClient")), 0)
        in_way_from = max(_to_int(stock.get("inWayFromClient")), 0)
        by_variant.setdefault(variant_key, []).append((dt, qty, in_way_to, in_way_from))

    out: Dict[Tuple[int, str], Dict[str, Any]] = {}
    for variant_key, events in by_variant.items():
        events.sort(key=lambda item: item[0])
        nm_id = variant_key[0]
        cur_qty = 0
        cur_to = 0
        cur_from = 0
        idx = 0
        for day in days:
            while idx < len(events) and events[idx][0] <= day:
                cur_qty = events[idx][1]
                cur_to = events[idx][2]
                cur_from = events[idx][3]
                idx += 1
            key = (nm_id, day)
            row = out.setdefault(
                key,
                {
                    "date": day,
                    "nm_id": nm_id,
                    "stocks_wb": 0,
                    "in_way_to_client": 0,
                    "in_way_from_client": 0,
                    "stocks_mp": 0,
                },
            )
            row["stocks_wb"] += max(cur_qty, 0)
            row["in_way_to_client"] += max(cur_to, 0)
            row["in_way_from_client"] += max(cur_from, 0)
            row["stocks_mp"] += max(cur_to + cur_from, 0)
    return list(out.values())


def _build_stock_snapshot_rows(
    stock_rows: List[Dict[str, Any]],
    day: str,
    nm_filter: Set[int],
) -> List[Dict[str, Any]]:
    """Build a *single-day* stock snapshot per nm_id.

    WB /stocks is effectively current-state. Persist a per-day snapshot and let dataset builders
    carry-forward the latest known snapshot when building historical rows.
    """

    dt = _parse_ymd(day) or str(day or "")[:10]
    out: Dict[int, Dict[str, Any]] = {}
    for stock in stock_rows:
        nm_id = _to_int(stock.get("nmId") or stock.get("nmID"))
        if not nm_id:
            continue
        if nm_filter and nm_id not in nm_filter:
            continue
        qty = max(_to_int(stock.get("quantity")), 0)
        in_way_to = max(_to_int(stock.get("inWayToClient")), 0)
        in_way_from = max(_to_int(stock.get("inWayFromClient")), 0)
        row = out.setdefault(
            nm_id,
            {
                "date": dt,
                "nm_id": nm_id,
                "stocks_wb": 0,
                "in_way_to_client": 0,
                "in_way_from_client": 0,
                # Back-compat: historically this field carried inWay sum.
                "stocks_mp": 0,
                "source": "wb_stats_snapshot",
            },
        )
        row["stocks_wb"] += qty
        row["in_way_to_client"] += in_way_to
        row["in_way_from_client"] += in_way_from
        row["stocks_mp"] += in_way_to + in_way_from
    return list(out.values())


def _build_adv_daily_rows(
    token: str,
    date_from: str,
    date_to: str,
    nm_filter: Set[int],
) -> List[Dict[str, Any]]:
    advert_items = fetch_advert_items(
        token, nm_ids=sorted(nm_filter) if nm_filter else None
    )
    if not advert_items:
        return []

    days = _date_range(date_from, date_to)
    day_set = set(days)

    meta_map: Dict[Tuple[int, int], Dict[str, Any]] = {}
    advert_to_nms: Dict[int, Set[int]] = {}
    for item in advert_items:
        advert_id = _to_int(item.get("advert_id"))
        nm_id = _to_int(item.get("nm_id"))
        if not advert_id or not nm_id:
            continue
        if nm_filter and nm_id not in nm_filter:
            continue
        meta_map[(advert_id, nm_id)] = item
        advert_to_nms.setdefault(advert_id, set()).add(nm_id)
    if not advert_to_nms:
        return []

    # Weight allocation for advert_id/day/nm_id:
    # 1) WB /adv/v3/fullstats (preferred money source in hybrid mode)
    # 2) adv_upd distributed by daily weights (fallback or explicit mode)
    # 3) normquery daily stats (fallback for missing weights only)
    daily_weight_map: Dict[Tuple[int, str], Dict[int, float]] = {}
    fullstats_spend_map: Dict[Tuple[int, str, int], float] = {}
    fullstats_total_map: Dict[Tuple[int, str], float] = {}
    month_upd_rows: List[Dict[str, Any]] = []
    month_advert_ids: Dict[Tuple[str, str], Set[int]] = {}

    for df, dt in _month_chunks(date_from, date_to):
        try:
            part = fetch_adv_upd(token, df, dt)
        except Exception:
            part = []
        ids_bucket = month_advert_ids.setdefault((df, dt), set())
        for item in part:
            if not isinstance(item, dict):
                continue
            month_upd_rows.append(item)
            aid = _to_int(
                item.get("advert_id") or item.get("advertId") or item.get("advertID")
            )
            if aid and aid in advert_to_nms:
                ids_bucket.add(aid)

    # Primary: fullstats rows per advert/day/nm.
    for (df, dt), ids in month_advert_ids.items():
        if not ids:
            continue
        try:
            stats_rows = fetch_adv_fullstats(token, sorted(ids), df, dt)
        except Exception:
            stats_rows = []
        flat_rows = build_adv_fullstats_daily_nm_spend(stats_rows)
        for item in flat_rows:
            if not isinstance(item, dict):
                continue
            advert_id = _to_int(item.get("advert_id"))
            nm_id = _to_int(item.get("nm_id"))
            day = _parse_ymd(item.get("date"))
            spend = max(_to_float(item.get("spend")), 0.0)
            if not advert_id or not nm_id or not day or spend <= 0:
                continue
            if day_set and day not in day_set:
                continue
            if nm_filter and nm_id not in nm_filter:
                continue
            key = (advert_id, day)
            bucket = daily_weight_map.setdefault(key, {})
            bucket[nm_id] = bucket.get(nm_id, 0.0) + spend
            fs_key = (advert_id, day, nm_id)
            fullstats_spend_map[fs_key] = fullstats_spend_map.get(fs_key, 0.0) + spend
            fullstats_total_map[key] = fullstats_total_map.get(key, 0.0) + spend

    # Fallback for missing fullstats weights: normquery daily stats.
    try:
        normquery_daily = fetch_normquery_daily_stats(
            token, date_from, date_to, advert_items
        )
    except Exception:
        normquery_daily = []
    for item in normquery_daily:
        if not isinstance(item, dict):
            continue
        advert_id = _to_int(
            item.get("advert_id") or item.get("advertId") or item.get("advertID")
        )
        nm_id = _to_int(item.get("nm_id") or item.get("nmId") or item.get("nmID"))
        if not advert_id or not nm_id:
            continue
        if nm_filter and nm_id not in nm_filter:
            continue
        daily_stats = item.get("dailyStats") or item.get("daily_stats") or []
        if not isinstance(daily_stats, list):
            continue
        for daily in daily_stats:
            if not isinstance(daily, dict):
                continue
            day = _parse_ymd(daily.get("date"))
            if not day or (day_set and day not in day_set):
                continue
            clusters = daily.get("stat") or daily.get("stats") or []
            if not isinstance(clusters, list):
                continue
            spend_sum = 0.0
            for cluster in clusters:
                if not isinstance(cluster, dict):
                    continue
                spend_sum += max(
                    _to_float(cluster.get("spend") or cluster.get("sum")), 0.0
                )
            if spend_sum <= 0:
                continue
            key = (advert_id, day)
            bucket = daily_weight_map.setdefault(key, {})
            if nm_id not in bucket:
                bucket[nm_id] = spend_sum

    advert_weight_days: Dict[int, List[str]] = {}
    for advert_id, day in daily_weight_map.keys():
        advert_weight_days.setdefault(advert_id, []).append(day)
    for advert_id in advert_weight_days.keys():
        advert_weight_days[advert_id] = sorted(set(advert_weight_days[advert_id]))

    def _weights_for_advert_day(advert_id: int, day: str) -> Dict[int, float]:
        direct = daily_weight_map.get((advert_id, day))
        if direct:
            return direct
        days_for_advert = advert_weight_days.get(advert_id) or []
        if not days_for_advert:
            return {}
        pos = bisect.bisect_right(days_for_advert, day)
        candidates: List[str] = []
        if pos > 0:
            candidates.append(days_for_advert[pos - 1])
        if pos < len(days_for_advert):
            candidates.append(days_for_advert[pos])
        for candidate in candidates:
            sample = daily_weight_map.get((advert_id, candidate))
            if sample:
                return sample
        return {}

    upd_amount_map: Dict[Tuple[int, str], float] = {}
    for upd in month_upd_rows:
        if not isinstance(upd, dict):
            continue
        advert_id = _to_int(
            upd.get("advert_id") or upd.get("advertId") or upd.get("advertID")
        )
        if not advert_id:
            continue
        if advert_id not in advert_to_nms:
            continue
        day = _parse_ymd(upd.get("upd_time") or upd.get("updTime"))
        if not day:
            continue
        if day_set and day not in day_set:
            try:
                day_dt = datetime.strptime(day, "%Y-%m-%d")
                start_dt = datetime.strptime(date_from[:10], "%Y-%m-%d")
                end_dt = datetime.strptime(date_to[:10], "%Y-%m-%d")
            except ValueError:
                continue
            if day_dt == end_dt + timedelta(days=1):
                day = end_dt.strftime("%Y-%m-%d")
            elif day_dt == start_dt - timedelta(days=1):
                day = start_dt.strftime("%Y-%m-%d")
            else:
                continue
        amount = _to_float(upd.get("upd_sum") or upd.get("updSum"))
        if abs(amount) <= 1e-9:
            continue
        key = (advert_id, day)
        upd_amount_map[key] = upd_amount_map.get(key, 0.0) + amount

    agg: Dict[Tuple[int, str], Dict[str, float]] = {}

    def _add_share(advert_id: int, nm_id: int, day: str, share: float) -> None:
        if abs(share) <= 1e-9:
            return
        key = (nm_id, day)
        bucket = agg.setdefault(
            key,
            {
                "adv_sum_total": 0.0,
                "adv_sum_auto": 0.0,
                "adv_sum_search": 0.0,
                "adv_sum_unknown": 0.0,
            },
        )
        bucket["adv_sum_total"] += share
        camp_type = campaign_bucket(meta_map.get((advert_id, nm_id), {}))
        if camp_type == "auto":
            bucket["adv_sum_auto"] += share
        elif camp_type == "search":
            bucket["adv_sum_search"] += share
        else:
            bucket["adv_sum_unknown"] += share

    source_mode = ADV_DAILY_MONEY_SOURCE or "hybrid"

    # 1) Fullstats as direct source (hybrid/fullstats modes).
    if source_mode in {"hybrid", "fullstats"}:
        for (advert_id, day, nm_id), spend in fullstats_spend_map.items():
            if nm_filter and nm_id not in nm_filter:
                continue
            nms = advert_to_nms.get(advert_id) or set()
            if nm_id not in nms:
                continue
            _add_share(advert_id, nm_id, day, spend)

    # 2) UPD as direct source (upd mode) or fallback for gaps (hybrid mode).
    for (advert_id, day), amount in upd_amount_map.items():
        has_fullstats = abs(fullstats_total_map.get((advert_id, day), 0.0)) > 1e-9
        if source_mode == "fullstats":
            use_upd = False
        elif source_mode == "upd":
            use_upd = True
        elif source_mode == "hybrid":
            use_upd = not has_fullstats
        else:
            use_upd = not has_fullstats
        if not use_upd:
            continue

        nm_ids = advert_to_nms.get(advert_id) or set()
        nm_ids_list = sorted(
            [nm for nm in nm_ids if (not nm_filter or nm in nm_filter)]
        )
        if not nm_ids_list:
            continue
        weights = _weights_for_advert_day(advert_id, day)
        weights_total = 0.0
        for nm_id in nm_ids_list:
            weights_total += max(_to_float(weights.get(nm_id)), 0.0)

        if weights_total > 0:
            for nm_id in nm_ids_list:
                weight = max(_to_float(weights.get(nm_id)), 0.0)
                share = amount * (weight / weights_total)
                _add_share(advert_id, nm_id, day, share)
        else:
            even = amount / float(len(nm_ids_list))
            for nm_id in nm_ids_list:
                _add_share(advert_id, nm_id, day, even)

    source_name = "adv_fullstats_or_upd_weighted"
    if source_mode == "upd":
        source_name = "adv_upd_weighted"
    elif source_mode == "fullstats":
        source_name = "adv_fullstats"

    out: List[Dict[str, Any]] = []
    for (nm_id, day), values in agg.items():
        out.append(
            {
                "date": day,
                "nm_id": nm_id,
                "adv_sum_total": round(values["adv_sum_total"], 2),
                "adv_sum_auto": round(values["adv_sum_auto"], 2),
                "adv_sum_search": round(values["adv_sum_search"], 2),
                "adv_sum_unknown": round(values["adv_sum_unknown"], 2),
                "source": source_name,
            }
        )
    return out


def _split_nm_ids(raw: str) -> List[int]:
    out: List[int] = []
    for chunk in str(raw or "").split(","):
        val = _to_int(chunk.strip())
        if val:
            out.append(val)
    return out


def _fallback_query_from_card(card: Dict[str, Any]) -> str:
    title = str(card.get("title") or "").strip().lower()
    brand = str(card.get("brand_name") or card.get("brand") or "").strip().lower()
    text = title if title else brand
    if not text:
        return ""
    tokens = [
        tok for tok in text.replace("/", " ").replace("-", " ").split() if len(tok) >= 3
    ]
    if not tokens:
        return ""
    return " ".join(tokens[:2]).strip()


def run_collection(
    storage: Storage,
    spreadsheet_id: str,
    date_from: str,
    date_to: str,
    nm_ids: Optional[List[int]] = None,
    with_positions: bool = True,
    with_funnel: bool = True,
) -> Dict[str, Any]:
    day_from = str(date_from or "")[:10]
    day_to = str(date_to or "")[:10]
    days = _date_range(day_from, day_to)
    if not days:
        raise ValueError("invalid date range")

    active_tokens = [
        item for item in storage.list_wb_tokens(spreadsheet_id) if item.is_active
    ]
    if not active_tokens:
        raise ValueError("no active WB token for spreadsheet")

    requested_nm_set = set([int(v) for v in (nm_ids or []) if int(v)])

    summary = {
        "spreadsheets": 1,
        "tokens": len(active_tokens),
        "date_from": day_from,
        "date_to": day_to,
        "rows": {
            "prices": 0,
            "stocks": 0,
            "adv": 0,
            "localization": 0,
            "funnel": 0,
            "detail_history": 0,
            "search_keys": 0,
            "positions": 0,
            "tariffs_commission": 0,
            "tariffs_box": 0,
        },
    }

    # Tariffs/commissions are global reference data and can change over time.
    # Collect once per run (using the first token that succeeds).
    commission_rows: List[Dict[str, Any]] = []
    for token_item in active_tokens:
        try:
            commission_rows = fetch_tariffs_commission(token_item.token, locale="ru")
        except Exception:
            commission_rows = []
        if commission_rows:
            break
    if commission_rows:
        summary["rows"]["tariffs_commission"] = storage.upsert_wb_commission_rates(
            day_to, commission_rows
        )

    box_rows: List[Dict[str, Any]] = []
    for token_item in active_tokens:
        try:
            box_rows = fetch_tariffs_box(token_item.token, day_to)
        except Exception:
            box_rows = []
        if box_rows:
            break
    if box_rows:
        summary["rows"]["tariffs_box"] = storage.upsert_wb_box_tariffs(day_to, box_rows)

    for token_item in active_tokens:
        token = token_item.token

        try:
            cards = fetch_cards(token, token_item.sid)
        except Exception:
            cards = []

        nm_set = requested_nm_set.copy()
        if not nm_set:
            for card in cards:
                nm_id = _to_int(card.get("nm_id"))
                if nm_id:
                    nm_set.add(nm_id)

        # Price snapshot is current-state only. Save for date_to (collector run day).
        price_rows: List[Dict[str, Any]] = []
        for card in cards:
            nm_id = _to_int(card.get("nm_id"))
            if not nm_id:
                continue
            if nm_set and nm_id not in nm_set:
                continue
            discounted_price = max(_to_float(card.get("discounted_price")), 0.0)
            spp = max(_to_float(card.get("spp")), 0.0)
            if spp > 1.0:
                spp = spp / 100.0
            spp = max(0.0, min(0.95, spp))
            price_rows.append(
                {
                    "date": day_to,
                    "nm_id": nm_id,
                    "discounted_price": round(discounted_price, 2),
                    "discounted_price_with_spp": round(
                        discounted_price * (1.0 - spp), 2
                    ),
                    "spp": round(spp, 6),
                    "source": "cards",
                }
            )
        summary["rows"]["prices"] += storage.upsert_daily_prices(
            spreadsheet_id, price_rows
        )

        # Stocks / in-way snapshot:
        # - WB /stocks is current-state; it does not provide true historical snapshots.
        # - We persist a per-day snapshot (date_to) and carry it forward when building historical rows.
        # - Legacy mode "last_change" exists for parity experiments but should not be relied on as history.
        stocks_source = "wb_stats"
        stocks_from = day_from
        mode = STOCKS_MODE
        if mode in {"", "snapshot"}:
            stocks_source = "wb_stats_snapshot"
            try:
                dt_to = datetime.strptime(day_to, "%Y-%m-%d")
                lookback_days = max(int(STOCKS_SNAPSHOT_LOOKBACK_DAYS), 1)
                stocks_from = (dt_to - timedelta(days=lookback_days)).strftime(
                    "%Y-%m-%d"
                )
            except ValueError:
                stocks_from = day_from
        try:
            stocks_raw = fetch_stocks(token, stocks_from)
        except Exception:
            stocks_raw = []
        if mode in {"last_change", "events", "event"}:
            stock_rows = _build_stock_daily_rows(stocks_raw, days, nm_set)
        else:
            stock_rows = _build_stock_snapshot_rows(stocks_raw, day_to, nm_set)
        summary["rows"]["stocks"] += storage.upsert_daily_stocks(
            spreadsheet_id, stock_rows, source=stocks_source
        )

        # Daily advertising spend by SKU + bucket from adv_upd.
        adv_rows = _build_adv_daily_rows(token, day_from, day_to, nm_set)
        summary["rows"]["adv"] += storage.upsert_daily_adv(spreadsheet_id, adv_rows)

        # Daily regional localization from factual orders rows.
        try:
            orders_rows = fetch_orders(token, day_from, day_to)
        except Exception:
            orders_rows = []
        localization_rows = _build_localization_daily_rows(orders_rows, nm_set)
        summary["rows"]["localization"] += storage.upsert_daily_localization(
            spreadsheet_id, localization_rows
        )

        # Funnel (open cards / cart / wishlist) for daily checklist inputs.
        if with_funnel and nm_set:
            try:
                funnel_rows = fetch_sales_funnel_history(
                    token, day_from, day_to, sorted(nm_set)
                )
            except Exception:
                funnel_rows = []
            summary["rows"]["funnel"] += storage.upsert_daily_funnel(
                spreadsheet_id, funnel_rows
            )
            if DETAIL_HISTORY_ENABLED:
                try:
                    detail_history_rows = fetch_detail_history_report_csv(
                        token, day_from, day_to, sorted(nm_set)
                    )
                except Exception:
                    detail_history_rows = []
                summary["rows"]["detail_history"] += (
                    storage.upsert_daily_detail_history(
                        spreadsheet_id, detail_history_rows
                    )
                )

        # Search keys from adv normquery and search positions parser.
        adv_keys_from = (
            datetime.strptime(day_to, "%Y-%m-%d") - timedelta(days=30)
        ).strftime("%Y-%m-%d")
        try:
            adv_rows_for_keys = build_adv_normquery_rows(
                token,
                adv_keys_from,
                day_to,
                nm_ids=sorted(nm_set) if nm_set else None,
                min_views=10,
            )
        except Exception:
            adv_rows_for_keys = []
        search_key_rows = build_search_items_from_adv_rows(
            adv_rows_for_keys, max_keys_per_nm=8, min_views=10
        )
        normalized_search_key_rows: List[Dict[str, Any]] = []
        for item in search_key_rows:
            if not isinstance(item, dict):
                continue
            nm_id = _to_int(
                item.get("nm_id") or item.get("itemNumber") or item.get("nmId")
            )
            query = str(item.get("search_key") or item.get("query") or "").strip()
            if not nm_id or not query:
                continue
            normalized_search_key_rows.append(
                {
                    "nm_id": nm_id,
                    "search_key": query,
                    "search_key_id": f"{nm_id}__{query}",
                    "source": "adv_normquery",
                }
            )
        if not normalized_search_key_rows:
            # Fallback: seed at least one key per SKU from card title to unlock
            # search position snapshots when adv normquery is not available.
            seen_ids: Set[int] = set()
            for card in cards:
                nm_id = _to_int(card.get("nm_id"))
                if not nm_id:
                    continue
                if nm_set and nm_id not in nm_set:
                    continue
                if nm_id in seen_ids:
                    continue
                query = _fallback_query_from_card(card)
                if not query:
                    continue
                seen_ids.add(nm_id)
                normalized_search_key_rows.append(
                    {
                        "nm_id": nm_id,
                        "search_key": query,
                        "search_key_id": f"{nm_id}__{query}",
                        "source": "card_title_fallback",
                    }
                )
        summary["rows"]["search_keys"] += storage.upsert_search_keys(
            spreadsheet_id, normalized_search_key_rows, source="adv_normquery"
        )

        if with_positions:
            keys = storage.list_search_keys(
                spreadsheet_id, nm_ids=sorted(nm_set) if nm_set else None
            )
            items = [
                {"itemNumber": int(row["nm_id"]), "query": str(row["search_key"])}
                for row in keys
                if int(row.get("nm_id") or 0)
                and str(row.get("search_key") or "").strip()
            ]
            pos_rows_raw, geo_rows = fetch_search_positions_multi(items)
            pos_rows: List[Dict[str, Any]] = []
            for row in pos_rows_raw:
                if not isinstance(row, list) or len(row) < 5:
                    continue
                nm_id = _to_int(row[1])
                if not nm_id:
                    continue
                pos_rows.append(
                    {
                        "date": day_to,
                        "captured_at": str(row[0] or ""),
                        "nm_id": nm_id,
                        "search_key": str(row[2] or ""),
                        "position": _to_int(row[3]),
                        "promo_position": _to_int(row[4]),
                        "source": "wb_search",
                    }
                )
            summary["rows"]["positions"] += storage.upsert_daily_positions(
                spreadsheet_id, day_to, pos_rows, source="wb_search"
            )
            if geo_rows:
                try:
                    summary["rows"]["positions_geo"] = summary["rows"].get(
                        "positions_geo", 0
                    ) + storage.upsert_daily_positions_geo(
                        spreadsheet_id, day_to, geo_rows, source="wb_search"
                    )
                except Exception:
                    pass

    return summary


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Collect WB snapshots for autonomous backend"
    )
    parser.add_argument("--spreadsheet-id", required=True, help="Target spreadsheet id")
    parser.add_argument("--date-from", default="", help="YYYY-MM-DD (default=today)")
    parser.add_argument(
        "--date-to", default="", help="YYYY-MM-DD (default=date-from or today)"
    )
    parser.add_argument("--nm-ids", default="", help="Comma-separated nm_ids")
    parser.add_argument(
        "--no-positions", action="store_true", help="Skip search positions parsing"
    )
    parser.add_argument(
        "--no-funnel", action="store_true", help="Skip sales funnel history collection"
    )
    parser.add_argument(
        "--db-path",
        default="",
        help="Optional custom DB path (default from backend storage config)",
    )
    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    today = datetime.utcnow().strftime("%Y-%m-%d")
    date_from = str(args.date_from or today)[:10]
    date_to = str(args.date_to or date_from or today)[:10]
    nm_ids = _split_nm_ids(args.nm_ids)

    db_path = str(args.db_path or "").strip()
    if db_path:
        storage = Storage(db_path)
    else:
        import os

        default_db = os.path.join(os.path.dirname(__file__), "data", "btlz.db")
        storage = Storage(os.environ.get("BTLZ_DB_PATH", default_db))

    summary = run_collection(
        storage=storage,
        spreadsheet_id=str(args.spreadsheet_id),
        date_from=date_from,
        date_to=date_to,
        nm_ids=nm_ids if nm_ids else None,
        with_positions=not bool(args.no_positions),
        with_funnel=not bool(args.no_funnel),
    )
    import json

    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
