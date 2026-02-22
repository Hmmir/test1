import json
import os
import re
import hashlib
import time
import io
import csv
import uuid
import zipfile
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from urllib_open import open_url


CONTENT_API_URL = "https://content-api.wildberries.ru/content/v2/get/cards/list"
STATS_API_BASE = "https://statistics-api.wildberries.ru/api/v1/supplier"
REPORT_DETAIL_URLS = [
    "https://statistics-api.wildberries.ru/api/v5/supplier/reportDetailByPeriod",
    "https://statistics-api.wildberries.ru/api/v1/supplier/reportDetailByPeriod",
]
SALES_FUNNEL_URL = (
    "https://seller-analytics-api.wildberries.ru/api/analytics/v3/sales-funnel/products"
)
SALES_FUNNEL_HISTORY_URL = "https://seller-analytics-api.wildberries.ru/api/analytics/v3/sales-funnel/products/history"
SELLER_ANALYTICS_DOWNLOADS_URL = (
    "https://seller-analytics-api.wildberries.ru/api/v2/nm-report/downloads"
)
DETAIL_HISTORY_REPORT_TYPE = str(
    os.environ.get("BTLZ_DETAIL_HISTORY_REPORT_TYPE", "DETAIL_HISTORY_REPORT")
).strip()
ADV_PROMOTION_COUNT_URL = "https://advert-api.wildberries.ru/adv/v1/promotion/count"
ADV_ADVERTS_URL = "https://advert-api.wildberries.ru/api/advert/v2/adverts"
ADV_NORMQUERY_STATS_URL = "https://advert-api.wildberries.ru/adv/v0/normquery/stats"
ADV_NORMQUERY_DAILY_STATS_URL = (
    "https://advert-api.wildberries.ru/adv/v1/normquery/stats"
)
ADV_UPD_URL = "https://advert-api.wildberries.ru/adv/v1/upd"
ADV_FULLSTATS_URL = "https://advert-api.wildberries.ru/adv/v3/fullstats"
CARD_PUBLIC_DETAIL_URL = "https://card.wb.ru/cards/v4/detail"
TARIFFS_API_BASE = "https://common-api.wildberries.ru/api/v1/tariffs"
SEARCH_API_URLS = [
    "https://search.wb.ru/exactmatch/ru/common/v4/search",
    "https://search.wb.ru/exactmatch/ru/common/v13/search",
]
SALES_FUNNEL_CHUNK = int(os.environ.get("BTLZ_SALES_FUNNEL_CHUNK", "20"))
# History endpoint is usually rate-limited and can reject large date ranges.
# We chunk both nmIds and dates; the max requests cap prevents accidental overload.
SALES_FUNNEL_MAX_REQUESTS = int(os.environ.get("BTLZ_SALES_FUNNEL_MAX_REQUESTS", "40"))
# Empirically, WB sales-funnel history rejects ranges longer than ~8 days (inclusive).
SALES_FUNNEL_MAX_DAYS = int(os.environ.get("BTLZ_SALES_FUNNEL_MAX_DAYS", "8"))
DETAIL_HISTORY_NM_CHUNK = int(os.environ.get("BTLZ_DETAIL_HISTORY_NM_CHUNK", "40"))
DETAIL_HISTORY_MAX_DAYS = int(os.environ.get("BTLZ_DETAIL_HISTORY_MAX_DAYS", "14"))
DETAIL_HISTORY_MAX_REQUESTS = int(
    os.environ.get("BTLZ_DETAIL_HISTORY_MAX_REQUESTS", "60")
)
DETAIL_HISTORY_POLL_TIMEOUT_SECONDS = int(
    os.environ.get("BTLZ_DETAIL_HISTORY_POLL_TIMEOUT_SECONDS", "120")
)
DETAIL_HISTORY_POLL_INTERVAL_SECONDS = float(
    os.environ.get("BTLZ_DETAIL_HISTORY_POLL_INTERVAL_SECONDS", "1.0")
)
ADV_ADVERT_IDS_CHUNK = int(os.environ.get("BTLZ_ADV_ADVERT_IDS_CHUNK", "50"))
ADV_FULLSTATS_IDS_CHUNK = int(os.environ.get("BTLZ_ADV_FULLSTATS_IDS_CHUNK", "50"))
ADV_MAX_ITEMS = int(os.environ.get("BTLZ_ADV_MAX_ITEMS", "500"))
WB_SEARCH_MAX_PAGES = int(os.environ.get("BTLZ_WB_SEARCH_MAX_PAGES", "20"))
WB_SEARCH_TIMEOUT = int(os.environ.get("BTLZ_WB_SEARCH_TIMEOUT", "20"))
WB_CARD_PUBLIC_CHUNK = int(os.environ.get("BTLZ_WB_CARD_PUBLIC_CHUNK", "100"))
ADV_SEARCH_TYPE_CODES = {
    int(code.strip())
    for code in os.environ.get("BTLZ_ADV_SEARCH_TYPE_CODES", "6,7,8,9").split(",")
    if code.strip().isdigit()
}
ADV_AUTO_TYPE_CODES = {
    int(code.strip())
    for code in os.environ.get("BTLZ_ADV_AUTO_TYPE_CODES", "4,5").split(",")
    if code.strip().isdigit()
}
ADV_AUTO_BID_TYPES = {
    t.strip().lower()
    for t in os.environ.get("BTLZ_ADV_AUTO_BID_TYPES", "unified").split(",")
    if t.strip()
}
ADV_SEARCH_BID_TYPES = {
    t.strip().lower()
    for t in os.environ.get("BTLZ_ADV_SEARCH_BID_TYPES", "manual").split(",")
    if t.strip()
}

# Basic, file-based cache + retry/backoff for WB API calls.
# This repo is frequently used in parity runs that can trigger WB 429 rate limits.
WB_CACHE_ENABLED = str(
    os.environ.get("BTLZ_WB_CACHE_ENABLED", "1")
).strip().lower() in {
    "1",
    "true",
    "yes",
}
WB_CACHE_TTL_SECONDS = int(os.environ.get("BTLZ_WB_CACHE_TTL_SECONDS", "300"))
WB_PUBLIC_CACHE_ENABLED = str(
    os.environ.get("BTLZ_WB_PUBLIC_CACHE_ENABLED", "1")
).strip().lower() in {"1", "true", "yes"}
WB_PUBLIC_CACHE_TTL_SECONDS = int(
    os.environ.get("BTLZ_WB_PUBLIC_CACHE_TTL_SECONDS", str(WB_CACHE_TTL_SECONDS))
)
WB_RETRY_MAX = int(os.environ.get("BTLZ_WB_RETRY_MAX", "6"))
WB_RETRY_BASE_SECONDS = float(os.environ.get("BTLZ_WB_RETRY_BASE_SECONDS", "1.0"))
WB_SEARCH_DEST = (
    str(os.environ.get("BTLZ_WB_SEARCH_DEST", "-1257786") or "").strip() or "-1257786"
)
WB_SEARCH_SPP = str(os.environ.get("BTLZ_WB_SEARCH_SPP", "30") or "").strip() or "30"
WB_SEARCH_SORT = (
    str(os.environ.get("BTLZ_WB_SEARCH_SORT", "popular") or "").strip() or "popular"
)
WB_SEARCH_SITE_DEFAULTS = str(
    os.environ.get("BTLZ_WB_SEARCH_SITE_DEFAULTS", "0") or ""
).strip().lower() in {"1", "true", "yes"}
WB_SEARCH_LOCALE = (
    str(os.environ.get("BTLZ_WB_SEARCH_LOCALE", "ru") or "").strip() or "ru"
)
WB_SEARCH_REGIONS = str(os.environ.get("BTLZ_WB_SEARCH_REGIONS", "") or "").strip()
WB_SEARCH_COUPONS_GEO = str(
    os.environ.get("BTLZ_WB_SEARCH_COUPONS_GEO", "") or ""
).strip()
WB_SEARCH_EMP = str(os.environ.get("BTLZ_WB_SEARCH_EMP", "") or "").strip()
WB_SEARCH_LOCATE = str(os.environ.get("BTLZ_WB_SEARCH_LOCATE", "") or "").strip()
WB_SEARCH_REG = str(os.environ.get("BTLZ_WB_SEARCH_REG", "") or "").strip()
WB_SEARCH_SPP_FIX_GEO = str(
    os.environ.get("BTLZ_WB_SEARCH_SPP_FIX_GEO", "") or ""
).strip()
WB_SEARCH_PRICE_MARGIN_COEFF = str(
    os.environ.get("BTLZ_WB_SEARCH_PRICE_MARGIN_COEFF", "") or ""
).strip()
if WB_SEARCH_SITE_DEFAULTS:
    # These params mirror what is commonly used by WB web/mobile clients and third-party scrapers.
    # They can materially affect result ordering and availability per region.
    if not WB_SEARCH_COUPONS_GEO:
        WB_SEARCH_COUPONS_GEO = "12,3,18,15,21"
    if not WB_SEARCH_EMP:
        WB_SEARCH_EMP = "0"
    if not WB_SEARCH_LOCATE:
        WB_SEARCH_LOCATE = "ru"
    if not WB_SEARCH_REG:
        WB_SEARCH_REG = "1"
    if not WB_SEARCH_REGIONS:
        WB_SEARCH_REGIONS = "80,68,64,83,4,38,33,70,82,69,86,75,30,40,48,1,22,66,31,71"
    if not WB_SEARCH_SPP_FIX_GEO:
        WB_SEARCH_SPP_FIX_GEO = "4"
    if not WB_SEARCH_PRICE_MARGIN_COEFF:
        WB_SEARCH_PRICE_MARGIN_COEFF = "1.0"
_raw_search_dests = str(os.environ.get("BTLZ_WB_SEARCH_DESTS", "") or "")
WB_SEARCH_DESTS = [d.strip() for d in _raw_search_dests.split(",") if d.strip()] or [
    WB_SEARCH_DEST
]
WB_SEARCH_POS_AGG = (
    str(os.environ.get("BTLZ_WB_SEARCH_POS_AGG", "first") or "").strip().lower()
    or "first"
)
try:
    WB_PUBLIC_DELAY_SECONDS = float(
        os.environ.get("BTLZ_WB_PUBLIC_DELAY_SECONDS", "0") or 0.0
    )
except Exception:
    WB_PUBLIC_DELAY_SECONDS = 0.0

_WB_CACHE_DIR = Path(
    os.environ.get(
        "BTLZ_WB_CACHE_DIR",
        str(Path(__file__).resolve().parent / "data" / "wb_cache"),
    )
)


def _cache_key(method: str, url: str, token: str, body: Optional[bytes]) -> str:
    h = hashlib.sha256()
    h.update(method.upper().encode("utf-8"))
    h.update(b"\0")
    h.update(url.encode("utf-8"))
    h.update(b"\0")
    # Token is included only in the hash; it is never written to disk.
    h.update(token.encode("utf-8"))
    h.update(b"\0")
    if body:
        h.update(body)
    return h.hexdigest()


def _cache_get(
    method: str, url: str, token: str, body: Optional[bytes]
) -> Optional[Any]:
    if not WB_CACHE_ENABLED:
        return None
    if WB_CACHE_TTL_SECONDS <= 0:
        return None
    try:
        key = _cache_key(method, url, token, body)
        path = _WB_CACHE_DIR / f"{key}.json"
        if not path.exists():
            return None
        age = time.time() - path.stat().st_mtime
        if age > float(WB_CACHE_TTL_SECONDS):
            return None
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _cache_put(
    method: str, url: str, token: str, body: Optional[bytes], payload: Any
) -> None:
    if not WB_CACHE_ENABLED:
        return
    if WB_CACHE_TTL_SECONDS <= 0:
        return
    try:
        key = _cache_key(method, url, token, body)
        _WB_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        path = _WB_CACHE_DIR / f"{key}.json"
        path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    except Exception:
        return


def _public_cache_key(url: str) -> str:
    h = hashlib.sha256()
    h.update(b"PUBLIC")
    h.update(b"\0")
    h.update(str(url).encode("utf-8"))
    return h.hexdigest()


def _public_cache_get(url: str) -> Optional[Any]:
    if not (WB_CACHE_ENABLED and WB_PUBLIC_CACHE_ENABLED):
        return None
    if WB_PUBLIC_CACHE_TTL_SECONDS <= 0:
        return None
    try:
        key = _public_cache_key(url)
        path = _WB_CACHE_DIR / f"pub_{key}.json"
        if not path.exists():
            return None
        age = time.time() - path.stat().st_mtime
        if age > float(WB_PUBLIC_CACHE_TTL_SECONDS):
            return None
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _public_cache_put(url: str, payload: Any) -> None:
    if not (WB_CACHE_ENABLED and WB_PUBLIC_CACHE_ENABLED):
        return
    if WB_PUBLIC_CACHE_TTL_SECONDS <= 0:
        return
    try:
        key = _public_cache_key(url)
        _WB_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        path = _WB_CACHE_DIR / f"pub_{key}.json"
        path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    except Exception:
        return


def _retry_sleep_seconds(attempt: int, retry_after_header: Optional[str]) -> float:
    if retry_after_header:
        try:
            val = float(str(retry_after_header).strip())
            if val > 0:
                # Add a small jitter to desynchronize bursts.
                return min(60.0, val + (0.05 * float(attempt)))
        except Exception:
            pass
    # Exponential backoff with a cap.
    return min(60.0, WB_RETRY_BASE_SECONDS * (2.0 ** float(max(attempt, 0))))


def _to_date(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    text = text.replace("Z", "+00:00")
    if len(text) >= 10:
        first = text[:10]
        try:
            return datetime.strptime(first, "%Y-%m-%d")
        except ValueError:
            pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def _post_json(url: str, payload: Dict[str, Any], token: str) -> Dict[str, Any]:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    cached = _cache_get("POST", url, token, body)
    if cached is not None and isinstance(cached, dict):
        return cached

    last_error: Optional[Exception] = None
    attempts = max(1, WB_RETRY_MAX)
    for attempt in range(attempts):
        req = urllib.request.Request(
            url,
            data=body,
            method="POST",
            headers={
                "Content-Type": "application/json",
                "Authorization": token,
            },
        )
        try:
            with open_url(req, timeout=30) as resp:
                raw = resp.read()
                if not raw:
                    payload_out: Dict[str, Any] = {}
                else:
                    payload_out = json.loads(raw.decode("utf-8"))
                _cache_put("POST", url, token, body, payload_out)
                return payload_out
        except urllib.error.HTTPError as exc:
            details = exc.read().decode("utf-8", "replace")
            last_error = RuntimeError(f"WB API {exc.code}: {details}")
            if exc.code == 429 and attempt < attempts - 1:
                time.sleep(
                    _retry_sleep_seconds(attempt, exc.headers.get("Retry-After"))
                )
                continue
            raise last_error
        except Exception as exc:
            last_error = exc
            if attempt < attempts - 1:
                time.sleep(_retry_sleep_seconds(attempt, None))
                continue
            raise

    if last_error:
        raise last_error
    return {}


def _get_json(url: str, token: str) -> Any:
    cached = _cache_get("GET", url, token, None)
    if cached is not None:
        return cached

    last_error: Optional[Exception] = None
    attempts = max(1, WB_RETRY_MAX)
    for attempt in range(attempts):
        req = urllib.request.Request(
            url,
            method="GET",
            headers={
                "Authorization": token,
            },
        )
        try:
            with open_url(req, timeout=30) as resp:
                raw = resp.read()
                if not raw:
                    payload_out: Any = []
                else:
                    payload_out = json.loads(raw.decode("utf-8"))
                _cache_put("GET", url, token, None, payload_out)
                return payload_out
        except urllib.error.HTTPError as exc:
            details = exc.read().decode("utf-8", "replace")
            last_error = RuntimeError(f"WB API {exc.code}: {details}")
            if exc.code == 429 and attempt < attempts - 1:
                time.sleep(
                    _retry_sleep_seconds(attempt, exc.headers.get("Retry-After"))
                )
                continue
            raise last_error
        except Exception as exc:
            last_error = exc
            if attempt < attempts - 1:
                time.sleep(_retry_sleep_seconds(attempt, None))
                continue
            raise

    if last_error:
        raise last_error
    return []


def _get_public_json(url: str, timeout: int = WB_SEARCH_TIMEOUT) -> Any:
    cached = _public_cache_get(url)
    if cached is not None:
        return cached

    last_error: Optional[Exception] = None
    attempts = max(1, WB_RETRY_MAX)
    for attempt in range(attempts):
        req = urllib.request.Request(
            url,
            method="GET",
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                "Accept": "application/json",
            },
        )
        try:
            with open_url(req, timeout=timeout) as resp:
                raw = resp.read()
            payload_out = {} if not raw else json.loads(raw.decode("utf-8"))
            _public_cache_put(url, payload_out)
            return payload_out
        except urllib.error.HTTPError as exc:
            details = exc.read().decode("utf-8", "replace")
            last_error = RuntimeError(f"WB public API {exc.code}: {details}")
            if exc.code == 429 and attempt < attempts - 1:
                time.sleep(
                    _retry_sleep_seconds(attempt, exc.headers.get("Retry-After"))
                )
                continue
            raise last_error
        except Exception as exc:
            last_error = exc
            if attempt < attempts - 1:
                time.sleep(_retry_sleep_seconds(attempt, None))
                continue
            raise

    if last_error:
        raise last_error
    return {}


def _request_json_uncached(
    method: str,
    url: str,
    token: str,
    payload: Optional[Dict[str, Any]] = None,
    timeout: int = 60,
) -> Any:
    method_norm = str(method or "GET").upper()
    body: Optional[bytes] = None
    headers: Dict[str, str] = {"Authorization": token}
    if payload is not None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        headers["Content-Type"] = "application/json"

    last_error: Optional[Exception] = None
    attempts = max(1, WB_RETRY_MAX)
    for attempt in range(attempts):
        req = urllib.request.Request(
            url,
            data=body if method_norm == "POST" else None,
            method=method_norm,
            headers=headers,
        )
        try:
            with open_url(req, timeout=timeout) as resp:
                raw = resp.read()
                if not raw:
                    return {}
                return json.loads(raw.decode("utf-8"))
        except urllib.error.HTTPError as exc:
            details = exc.read().decode("utf-8", "replace")
            last_error = RuntimeError(f"WB API {exc.code}: {details}")
            if exc.code == 429 and attempt < attempts - 1:
                time.sleep(
                    _retry_sleep_seconds(attempt, exc.headers.get("Retry-After"))
                )
                continue
            raise last_error
        except Exception as exc:
            last_error = exc
            if attempt < attempts - 1:
                time.sleep(_retry_sleep_seconds(attempt, None))
                continue
            raise

    if last_error:
        raise last_error
    return {}


def _request_bytes_uncached(
    url: str,
    token: str,
    timeout: int = 120,
) -> bytes:
    last_error: Optional[Exception] = None
    attempts = max(1, WB_RETRY_MAX)
    for attempt in range(attempts):
        req = urllib.request.Request(
            url,
            method="GET",
            headers={"Authorization": token},
        )
        try:
            with open_url(req, timeout=timeout) as resp:
                return resp.read()
        except urllib.error.HTTPError as exc:
            details = exc.read().decode("utf-8", "replace")
            last_error = RuntimeError(f"WB API {exc.code}: {details}")
            if exc.code == 429 and attempt < attempts - 1:
                time.sleep(
                    _retry_sleep_seconds(attempt, exc.headers.get("Retry-After"))
                )
                continue
            raise last_error
        except Exception as exc:
            last_error = exc
            if attempt < attempts - 1:
                time.sleep(_retry_sleep_seconds(attempt, None))
                continue
            raise
    if last_error:
        raise last_error
    return b""


def _in_range(
    item: Dict[str, Any], date_from: Optional[datetime], date_to: Optional[datetime]
) -> bool:
    if not date_from and not date_to:
        return True
    dt = _to_date(
        item.get("date") or item.get("lastChangeDate") or item.get("lastChangeDateTs")
    )
    if not dt:
        return True
    if date_from and dt < date_from:
        return False
    if date_to and dt > date_to:
        return False
    return True


def _nm_id(item: Dict[str, Any]) -> Optional[int]:
    raw = item.get("nmId")
    if raw is None:
        raw = item.get("nmID")
    if raw is None or raw == "":
        return None
    try:
        val = int(str(raw))
        return val if val else None
    except (TypeError, ValueError):
        return None


def _int_value(value: Any) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def _float_value(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _extract_basket_base(photo_url: str, nm_id: int) -> str:
    text = str(photo_url or "").strip()
    if text:
        match = re.search(
            r"(https://basket-\d+\.wbbasket\.ru/vol\d+/part\d+/\d+/)", text
        )
        if match:
            return match.group(1)
    if not nm_id:
        return ""
    part = nm_id // 1000
    vol = nm_id // 100000
    return f"https://basket-01.wbbasket.ru/vol{vol}/part{part}/{nm_id}/"


def _extract_discounted_price(
    item: Dict[str, Any], first_size: Dict[str, Any]
) -> float:
    candidates: List[Any] = [
        first_size.get("discountedPrice"),
        first_size.get("price"),
        first_size.get("priceKopeck"),
        first_size.get("priceKopecks"),
        item.get("discountedPrice"),
        item.get("price"),
        item.get("Price"),
    ]
    for key in ("price", "prices"):
        obj = first_size.get(key)
        if isinstance(obj, dict):
            candidates.extend(
                [
                    obj.get("discounted"),
                    obj.get("discount"),
                    obj.get("base"),
                    obj.get("price"),
                    obj.get("value"),
                ]
            )
        elif isinstance(obj, list):
            for part in obj:
                if isinstance(part, dict):
                    candidates.extend(
                        [
                            part.get("discounted"),
                            part.get("discount"),
                            part.get("base"),
                            part.get("price"),
                            part.get("value"),
                        ]
                    )
                else:
                    candidates.append(part)
        elif obj is not None:
            candidates.append(obj)

    for candidate in candidates:
        val = _float_value(candidate)
        if val > 0:
            if val > 100000:
                val = val / 100.0
            return round(val, 2)
    return 0.0


def _extract_public_card_price(product: Dict[str, Any]) -> float:
    if not isinstance(product, dict):
        return 0.0

    # Public WB cards endpoint usually keeps actual retail data in sizes[].price.
    raw_sizes = product.get("sizes")
    sizes: List[Any] = raw_sizes if isinstance(raw_sizes, list) else []
    for size in sizes:
        if not isinstance(size, dict):
            continue
        price_obj = size.get("price")
        if isinstance(price_obj, dict):
            for key in ("product", "total", "price", "basic", "value"):
                val = _float_value(price_obj.get(key))
                if val > 0:
                    # Public cards API returns kopecks for price fields.
                    return round(val / 100.0, 2)
        elif price_obj is not None:
            val = _float_value(price_obj)
            if val > 0:
                return round(val / 100.0, 2)

    for key in ("salePriceU", "salePrice", "priceU", "price"):
        val = _float_value(product.get(key))
        if val > 0:
            return round(val / 100.0, 2)
    return 0.0


def _fetch_public_card_prices(nm_ids: List[int]) -> Dict[int, float]:
    if not nm_ids:
        return {}

    out: Dict[int, float] = {}
    chunk = max(1, WB_CARD_PUBLIC_CHUNK)

    for idx in range(0, len(nm_ids), chunk):
        part = [int(nm) for nm in nm_ids[idx : idx + chunk] if int(nm)]
        if not part:
            continue

        params = {
            "appType": "1",
            "curr": "rub",
            "dest": "-1257786",
            "hide_dtype": "13",
            "spp": "30",
            "nm": ";".join(str(v) for v in part),
        }
        url = CARD_PUBLIC_DETAIL_URL + "?" + urllib.parse.urlencode(params)
        try:
            payload = _get_public_json(url)
        except Exception:
            continue

        products = payload.get("products") if isinstance(payload, dict) else None
        if not isinstance(products, list):
            continue
        for product in products:
            if not isinstance(product, dict):
                continue
            nm_id = _int_value(product.get("id") or product.get("nmId"))
            if not nm_id:
                continue
            price = _extract_public_card_price(product)
            if price > 0:
                out[nm_id] = round(price, 2)

    return out


def _normalize_timestamp(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if "T" in text:
        text = text.replace("T", " ")
    if "+" in text:
        text = text.split("+", 1)[0]
    if text.endswith("Z"):
        text = text[:-1]
    return text


def fetch_cards(
    token: str,
    sid: Optional[str],
    page_limit: int = 100,
    max_pages: int = 20,
) -> List[Dict[str, Any]]:
    if not token:
        return []

    cards: List[Dict[str, Any]] = []
    cursor: Dict[str, Any] = {"limit": page_limit}

    for _ in range(max_pages):
        payload = {
            "settings": {
                "sort": {"ascending": False},
                "filter": {"withPhoto": -1},
                "cursor": cursor,
            }
        }
        response = _post_json(CONTENT_API_URL, payload, token)
        batch = response.get("cards") if isinstance(response, dict) else None
        if not isinstance(batch, list) or not batch:
            break

        for item in batch:
            if not isinstance(item, dict):
                continue
            nm_id = _nm_id(item)
            if not nm_id:
                continue

            raw_dims = item.get("dimensions")
            dims: Dict[str, Any] = raw_dims if isinstance(raw_dims, dict) else {}
            raw_photos = item.get("photos")
            photos: List[Any] = raw_photos if isinstance(raw_photos, list) else []
            first_photo = photos[0] if photos and isinstance(photos[0], dict) else {}
            photo_url = str(first_photo.get("big") or first_photo.get("c516x688") or "")
            raw_sizes = item.get("sizes")
            sizes: List[Any] = raw_sizes if isinstance(raw_sizes, list) else []
            first_size = sizes[0] if sizes and isinstance(sizes[0], dict) else {}
            discounted_price = _extract_discounted_price(item, first_size)

            cards.append(
                {
                    "nm_id": nm_id,
                    "imt_id": _int_value(item.get("imtID")),
                    "subject_id": _int_value(item.get("subjectID")),
                    "created_at": _normalize_timestamp(item.get("createdAt")),
                    "updated_at": _normalize_timestamp(item.get("updatedAt")),
                    "sid": sid,
                    "vendor_code": item.get("vendorCode") or "",
                    "subject_name": item.get("subjectName") or "",
                    "brand_name": item.get("brand") or "",
                    "brand": item.get("brand") or "",
                    "title": item.get("title") or "",
                    "width": _int_value(dims.get("width")),
                    "height": _int_value(dims.get("height")),
                    "length": _int_value(dims.get("length")),
                    "basket": _extract_basket_base(photo_url, nm_id),
                    "discounted_price": round(discounted_price, 2),
                }
            )

        cursor_resp = response.get("cursor") if isinstance(response, dict) else {}
        if not isinstance(cursor_resp, dict):
            break
        updated_at = cursor_resp.get("updatedAt")
        nm_id_cursor = cursor_resp.get("nmID")
        if not updated_at:
            break
        cursor = {
            "limit": page_limit,
            "updatedAt": updated_at,
            "nmID": nm_id_cursor,
        }

        if len(batch) < page_limit:
            break

    dedup: Dict[int, Dict[str, Any]] = {}
    for card in cards:
        dedup[card["nm_id"]] = card

    if dedup:
        public_prices = _fetch_public_card_prices(sorted(dedup.keys()))
        for nm_id, price in public_prices.items():
            card = dedup.get(nm_id)
            if not card:
                continue
            if _float_value(card.get("discounted_price")) <= 0 and price > 0:
                card["discounted_price"] = round(price, 2)
    return list(dedup.values())


def fetch_orders(
    token: str, date_from: str, date_to: Optional[str]
) -> List[Dict[str, Any]]:
    if not token:
        return []
    query = urllib.parse.urlencode({"dateFrom": date_from})
    data = _get_json(f"{STATS_API_BASE}/orders?{query}", token)
    if not isinstance(data, list):
        return []
    dt_from = _to_date(date_from)
    dt_to = _to_date(date_to)
    return [
        item
        for item in data
        if isinstance(item, dict) and _in_range(item, dt_from, dt_to)
    ]


def fetch_sales(
    token: str, date_from: str, date_to: Optional[str]
) -> List[Dict[str, Any]]:
    if not token:
        return []
    query = urllib.parse.urlencode({"dateFrom": date_from})
    data = _get_json(f"{STATS_API_BASE}/sales?{query}", token)
    if not isinstance(data, list):
        return []
    dt_from = _to_date(date_from)
    dt_to = _to_date(date_to)
    return [
        item
        for item in data
        if isinstance(item, dict) and _in_range(item, dt_from, dt_to)
    ]


def fetch_sales_raw(token: str, date_from: str) -> List[Dict[str, Any]]:
    """Fetch raw /sales rows from WB Statistics API without client-side date filtering.

    The upstream endpoint filters by its own semantics of `dateFrom` and can include
    rows whose `lastChangeDate` is later than `date`. Callers can re-group by
    `lastChangeDate` for spreadsheet parity.
    """

    if not token:
        return []
    query = urllib.parse.urlencode({"dateFrom": date_from})
    data = _get_json(f"{STATS_API_BASE}/sales?{query}", token)
    if not isinstance(data, list):
        return []
    return [item for item in data if isinstance(item, dict)]


def fetch_stocks(token: str, date_from: str) -> List[Dict[str, Any]]:
    if not token:
        return []
    query = urllib.parse.urlencode({"dateFrom": date_from})
    data = _get_json(f"{STATS_API_BASE}/stocks?{query}", token)
    if not isinstance(data, list):
        return []
    return [item for item in data if isinstance(item, dict)]


def fetch_report_detail(
    token: str, date_from: str, date_to: str, page_limit: int = 100000
) -> List[Dict[str, Any]]:
    if not token:
        return []

    for endpoint in REPORT_DETAIL_URLS:
        try:
            rows: List[Dict[str, Any]] = []
            rrdid = 0
            for _ in range(20):
                query = urllib.parse.urlencode(
                    {
                        "dateFrom": date_from,
                        "dateTo": date_to,
                        "limit": page_limit,
                        "rrdid": rrdid,
                    }
                )
                data = _get_json(f"{endpoint}?{query}", token)
                if not isinstance(data, list) or not data:
                    break

                batch = [item for item in data if isinstance(item, dict)]
                rows.extend(batch)

                if len(batch) < page_limit:
                    break

                last_id_raw = batch[-1].get("rrd_id")
                if last_id_raw is None or last_id_raw == "":
                    break
                try:
                    last_id = int(str(last_id_raw))
                except (TypeError, ValueError):
                    break
                if last_id <= rrdid:
                    break
                rrdid = last_id

            return rows
        except Exception:
            continue

    return []


def fetch_tariffs_commission(token: str, locale: str = "ru") -> List[Dict[str, Any]]:
    """Fetch WB commission rates by subject (Tariffs API).

    Endpoint (official): GET https://common-api.wildberries.ru/api/v1/tariffs/commission?locale=ru
    """

    if not token:
        return []
    query = urllib.parse.urlencode({"locale": str(locale or "ru")})
    data = _get_json(f"{TARIFFS_API_BASE}/commission?{query}", token)
    if not isinstance(data, dict):
        return []
    report = data.get("report")
    return report if isinstance(report, list) else []


def fetch_tariffs_box(token: str, dt: str) -> List[Dict[str, Any]]:
    """Fetch box delivery/storage tariffs (Tariffs API).

    Endpoint (official): GET https://common-api.wildberries.ru/api/v1/tariffs/box?date=YYYY-MM-DD
    """

    day = str(dt or "")[:10]
    if not token or not day:
        return []
    query = urllib.parse.urlencode({"date": day})
    data = _get_json(f"{TARIFFS_API_BASE}/box?{query}", token)
    if not isinstance(data, dict):
        return []
    raw_resp = data.get("response")
    resp: Dict[str, Any] = raw_resp if isinstance(raw_resp, dict) else {}
    raw_payload = resp.get("data")
    payload: Dict[str, Any] = raw_payload if isinstance(raw_payload, dict) else {}
    warehouses = payload.get("warehouseList")
    return warehouses if isinstance(warehouses, list) else []


def fetch_sales_funnel(
    token: str,
    date_from: str,
    date_to: str,
    nm_ids: List[int],
    timezone: str = "Europe/Moscow",
) -> Dict[int, Dict[str, Any]]:
    if not token or not nm_ids:
        return {}

    result: Dict[int, Dict[str, Any]] = {}
    chunk_size = SALES_FUNNEL_CHUNK if SALES_FUNNEL_CHUNK > 0 else 20
    chunks = [nm_ids[i : i + chunk_size] for i in range(0, len(nm_ids), chunk_size)]
    requests_left = SALES_FUNNEL_MAX_REQUESTS

    for chunk in chunks:
        if requests_left <= 0:
            break
        requests_left -= 1

        payload = {
            "selectedPeriod": {"start": date_from, "end": date_to},
            "nmIds": chunk,
            "skipDeletedNm": True,
            "aggregationLevel": "day",
            "timezone": timezone,
        }

        try:
            data = _post_json(SALES_FUNNEL_URL, payload, token)
        except Exception:
            continue

        container = data.get("data") if isinstance(data, dict) else None
        products = container.get("products") if isinstance(container, dict) else None
        if not isinstance(products, list):
            continue

        for item in products:
            if not isinstance(item, dict):
                continue
            product_raw = item.get("product")
            product = product_raw if isinstance(product_raw, dict) else {}
            statistic_raw = item.get("statistic")
            statistic = statistic_raw if isinstance(statistic_raw, dict) else {}
            selected_raw = statistic.get("selected")
            selected = selected_raw if isinstance(selected_raw, dict) else {}
            nm_raw = product.get("nmId")
            if nm_raw is None or nm_raw == "":
                continue
            try:
                nm_id = int(str(nm_raw))
            except (TypeError, ValueError):
                continue
            result[nm_id] = dict(selected)

    return result


def fetch_sales_funnel_history(
    token: str,
    date_from: str,
    date_to: str,
    nm_ids: List[int],
    timezone: str = "Europe/Moscow",
) -> List[Dict[str, Any]]:
    if not token or not nm_ids:
        return []

    def _date_chunks(df: str, dt: str, max_days: int) -> List[Tuple[str, str]]:
        try:
            start = datetime.strptime(str(df)[:10], "%Y-%m-%d")
            end = datetime.strptime(str(dt)[:10], "%Y-%m-%d")
        except ValueError:
            return [(str(df)[:10], str(dt)[:10])] if str(df) and str(dt) else []
        if end < start:
            return []
        step = max(1, int(max_days))
        out: List[Tuple[str, str]] = []
        cur = start
        while cur <= end:
            chunk_end = cur + timedelta(days=step - 1)
            if chunk_end > end:
                chunk_end = end
            out.append((cur.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d")))
            cur = chunk_end + timedelta(days=1)
        return out

    rows: List[Dict[str, Any]] = []
    chunk_size = SALES_FUNNEL_CHUNK if SALES_FUNNEL_CHUNK > 0 else 20
    chunks = [nm_ids[i : i + chunk_size] for i in range(0, len(nm_ids), chunk_size)]
    requests_left = SALES_FUNNEL_MAX_REQUESTS
    day_chunks = _date_chunks(date_from, date_to, SALES_FUNNEL_MAX_DAYS)
    if not day_chunks:
        return []

    for chunk in chunks:
        for df, dt in day_chunks:
            if requests_left <= 0:
                break
            requests_left -= 1
            payload = {
                "selectedPeriod": {"start": df, "end": dt},
                "nmIds": chunk,
                "timezone": timezone,
            }
            try:
                data = _post_json(SALES_FUNNEL_HISTORY_URL, payload, token)
            except Exception:
                continue
            if not isinstance(data, list):
                continue
            for product_item in data:
                if not isinstance(product_item, dict):
                    continue
                raw_product = product_item.get("product")
                product: Dict[str, Any] = (
                    raw_product if isinstance(raw_product, dict) else {}
                )
                raw_history = product_item.get("history")
                history: List[Any] = (
                    raw_history if isinstance(raw_history, list) else []
                )
                nm_id = _int_value(product.get("nmId"))
                if not nm_id:
                    continue
                for daily in history:
                    if not isinstance(daily, dict):
                        continue
                    rows.append(
                        {
                            "nm_id": nm_id,
                            "date": str(daily.get("date") or "")[:10],
                            "vendor_code": str(product.get("vendorCode") or ""),
                            "title": str(product.get("title") or ""),
                            "brand_name": str(product.get("brandName") or ""),
                            "open_card_count": _int_value(daily.get("openCount")),
                            "add_to_cart_count": _int_value(daily.get("cartCount")),
                            "orders_count": _int_value(daily.get("orderCount")),
                            "orders_sum_rub": round(
                                _float_value(daily.get("orderSum")), 2
                            ),
                            "buyouts_count": _int_value(daily.get("buyoutCount")),
                            "buyouts_sum_rub": round(
                                _float_value(daily.get("buyoutSum")), 2
                            ),
                            "buyout_percent": round(
                                _float_value(daily.get("buyoutPercent")), 2
                            ),
                            "add_to_cart_conversion": round(
                                _float_value(daily.get("addToCartConversion")), 2
                            ),
                            "cart_to_order_conversion": round(
                                _float_value(daily.get("cartToOrderConversion")), 2
                            ),
                            "add_to_wishlist_count": _int_value(
                                daily.get("addToWishlistCount")
                            ),
                        }
                    )
    return rows


def _csv_float(value: Any) -> float:
    if value is None:
        return 0.0
    text = str(value).strip().replace(" ", "")
    if not text:
        return 0.0
    text = text.replace(",", ".")
    return _float_value(text)


def _date_chunks(date_from: str, date_to: str, max_days: int) -> List[Tuple[str, str]]:
    try:
        start = datetime.strptime(str(date_from)[:10], "%Y-%m-%d")
        end = datetime.strptime(str(date_to)[:10], "%Y-%m-%d")
    except ValueError:
        return []
    if end < start:
        return []
    out: List[Tuple[str, str]] = []
    step = max(1, int(max_days))
    cur = start
    while cur <= end:
        chunk_end = cur + timedelta(days=step - 1)
        if chunk_end > end:
            chunk_end = end
        out.append((cur.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d")))
        cur = chunk_end + timedelta(days=1)
    return out


def _parse_detail_history_zip(raw: bytes) -> List[Dict[str, Any]]:
    if not raw:
        return []
    try:
        with zipfile.ZipFile(io.BytesIO(raw)) as zf:
            names = [
                name for name in zf.namelist() if str(name).lower().endswith(".csv")
            ]
            if not names:
                return []
            out: List[Dict[str, Any]] = []
            for name in names:
                data = zf.read(name)
                text = data.decode("utf-8-sig", "replace")
                reader = csv.DictReader(io.StringIO(text))
                for row in reader:
                    if not isinstance(row, dict):
                        continue
                    nm_id = _int_value(
                        row.get("nmID") or row.get("nmId") or row.get("nm_id")
                    )
                    day = str(row.get("dt") or row.get("date") or "")[:10]
                    if not nm_id or not day:
                        continue
                    out.append(
                        {
                            "nm_id": nm_id,
                            "date": day,
                            "open_card_count": _int_value(
                                row.get("openCardCount") or row.get("openCount")
                            ),
                            "add_to_cart_count": _int_value(
                                row.get("addToCartCount") or row.get("cartCount")
                            ),
                            "add_to_wishlist_count": _int_value(
                                row.get("addToWishlist")
                                or row.get("addToWishlistCount")
                            ),
                            "orders_count": _int_value(
                                row.get("ordersCount")
                                or row.get("orderCount")
                                or row.get("orders")
                            ),
                            "orders_sum_rub": round(
                                _csv_float(
                                    row.get("ordersSumRub")
                                    or row.get("orderSum")
                                    or row.get("ordersSum")
                                ),
                                2,
                            ),
                            "buyouts_count": _int_value(
                                row.get("buyoutsCount") or row.get("buyoutCount")
                            ),
                            "buyouts_sum_rub": round(
                                _csv_float(
                                    row.get("buyoutsSumRub") or row.get("buyoutSum")
                                ),
                                2,
                            ),
                            "cancel_count": _int_value(row.get("cancelCount")),
                            "cancel_sum_rub": round(
                                _csv_float(row.get("cancelSumRub")), 2
                            ),
                            "add_to_cart_conversion": round(
                                _csv_float(row.get("addToCartConversion")),
                                2,
                            ),
                            "cart_to_order_conversion": round(
                                _csv_float(row.get("cartToOrderConversion")),
                                2,
                            ),
                            "buyout_percent": round(
                                _csv_float(row.get("buyoutPercent")), 2
                            ),
                            "currency": str(row.get("currency") or "").strip(),
                            "source": "detail_history_report",
                        }
                    )
            return out
    except Exception:
        return []


def _wait_report_success(token: str, report_id: str, timeout_seconds: int) -> bool:
    deadline = time.time() + max(5, int(timeout_seconds))
    poll_interval = max(0.2, float(DETAIL_HISTORY_POLL_INTERVAL_SECONDS))
    while time.time() < deadline:
        ts = int(time.time() * 1000)
        payload = _request_json_uncached(
            "GET",
            f"{SELLER_ANALYTICS_DOWNLOADS_URL}?_={ts}",
            token,
            timeout=60,
        )
        rows = payload.get("data") if isinstance(payload, dict) else None
        if isinstance(rows, list):
            for row in rows:
                if not isinstance(row, dict):
                    continue
                if str(row.get("id") or "") != str(report_id):
                    continue
                status = str(row.get("status") or "").upper()
                if status == "SUCCESS":
                    return True
                if status in {"ERROR", "FAILED"}:
                    return False
        time.sleep(poll_interval)
    return False


def fetch_detail_history_report_csv(
    token: str,
    date_from: str,
    date_to: str,
    nm_ids: List[int],
    timezone: str = "Europe/Moscow",
) -> List[Dict[str, Any]]:
    if not token or not nm_ids:
        return []
    uniq_nm_ids = sorted({int(v) for v in nm_ids if int(v)})
    if not uniq_nm_ids:
        return []

    nm_chunk_size = max(1, int(DETAIL_HISTORY_NM_CHUNK))
    date_chunks = _date_chunks(date_from, date_to, DETAIL_HISTORY_MAX_DAYS)
    if not date_chunks:
        return []
    nm_chunks = [
        uniq_nm_ids[idx : idx + nm_chunk_size]
        for idx in range(0, len(uniq_nm_ids), nm_chunk_size)
    ]
    max_requests = max(1, int(DETAIL_HISTORY_MAX_REQUESTS))
    requests_used = 0

    merged: Dict[Tuple[int, str], Dict[str, Any]] = {}
    for nm_chunk in nm_chunks:
        for day_from, day_to in date_chunks:
            if requests_used >= max_requests:
                break
            requests_used += 1
            report_id = str(uuid.uuid4())
            payload = {
                "id": report_id,
                "reportType": DETAIL_HISTORY_REPORT_TYPE,
                "userReportName": f"btlz-detail-history-{day_from}-{day_to}",
                "params": {
                    "startDate": day_from,
                    "endDate": day_to,
                    "timezone": timezone,
                    "aggregationLevel": "day",
                    "skipDeletedNm": True,
                    "nmIDs": nm_chunk,
                },
            }
            try:
                _request_json_uncached(
                    "POST",
                    SELLER_ANALYTICS_DOWNLOADS_URL,
                    token,
                    payload=payload,
                    timeout=60,
                )
            except Exception:
                continue

            ok = _wait_report_success(
                token,
                report_id,
                timeout_seconds=DETAIL_HISTORY_POLL_TIMEOUT_SECONDS,
            )
            if not ok:
                continue

            try:
                raw = _request_bytes_uncached(
                    f"{SELLER_ANALYTICS_DOWNLOADS_URL}/file/{report_id}",
                    token,
                    timeout=120,
                )
            except Exception:
                continue

            rows = _parse_detail_history_zip(raw)
            for row in rows:
                if not isinstance(row, dict):
                    continue
                nm_id = _int_value(row.get("nm_id"))
                day = str(row.get("date") or "")[:10]
                if not nm_id or not day:
                    continue
                key = (nm_id, day)
                prev = merged.get(key)
                if prev is None:
                    merged[key] = row
                    continue
                # Some report chunks can overlap; accumulate additive metrics and keep latest rates.
                prev["open_card_count"] = _int_value(
                    prev.get("open_card_count")
                ) + _int_value(row.get("open_card_count"))
                prev["add_to_cart_count"] = _int_value(
                    prev.get("add_to_cart_count")
                ) + _int_value(row.get("add_to_cart_count"))
                prev["add_to_wishlist_count"] = _int_value(
                    prev.get("add_to_wishlist_count")
                ) + _int_value(row.get("add_to_wishlist_count"))
                prev["orders_count"] = _int_value(
                    prev.get("orders_count")
                ) + _int_value(row.get("orders_count"))
                prev["orders_sum_rub"] = round(
                    _float_value(prev.get("orders_sum_rub"))
                    + _float_value(row.get("orders_sum_rub")),
                    2,
                )
                prev["buyouts_count"] = _int_value(
                    prev.get("buyouts_count")
                ) + _int_value(row.get("buyouts_count"))
                prev["buyouts_sum_rub"] = round(
                    _float_value(prev.get("buyouts_sum_rub"))
                    + _float_value(row.get("buyouts_sum_rub")),
                    2,
                )
                prev["cancel_count"] = _int_value(
                    prev.get("cancel_count")
                ) + _int_value(row.get("cancel_count"))
                prev["cancel_sum_rub"] = round(
                    _float_value(prev.get("cancel_sum_rub"))
                    + _float_value(row.get("cancel_sum_rub")),
                    2,
                )
                prev["add_to_cart_conversion"] = _float_value(
                    row.get("add_to_cart_conversion")
                )
                prev["cart_to_order_conversion"] = _float_value(
                    row.get("cart_to_order_conversion")
                )
                prev["buyout_percent"] = _float_value(row.get("buyout_percent"))
                if str(row.get("currency") or "").strip():
                    prev["currency"] = str(row.get("currency"))
        if requests_used >= max_requests:
            break

    return sorted(
        merged.values(),
        key=lambda item: (
            _int_value(item.get("nm_id")),
            str(item.get("date") or ""),
        ),
    )


def fetch_advert_items(
    token: str, nm_ids: Optional[List[int]] = None
) -> List[Dict[str, Any]]:
    if not token:
        return []

    nm_filter = set(nm_ids or [])
    try:
        payload = _get_json(ADV_PROMOTION_COUNT_URL, token)
    except Exception:
        return []
    groups = payload.get("adverts") if isinstance(payload, dict) else None
    if not isinstance(groups, list):
        return []

    advert_ids: List[int] = []
    for group in groups:
        if not isinstance(group, dict):
            continue
        advert_list = group.get("advert_list")
        if not isinstance(advert_list, list):
            continue
        for item in advert_list:
            if not isinstance(item, dict):
                continue
            advert_id = _int_value(item.get("advertId"))
            if advert_id:
                advert_ids.append(advert_id)

    if not advert_ids:
        return []

    unique_ids: List[int] = []
    seen_ids = set()
    for advert_id in advert_ids:
        if advert_id in seen_ids:
            continue
        seen_ids.add(advert_id)
        unique_ids.append(advert_id)

    out: List[Dict[str, Any]] = []
    step = max(1, ADV_ADVERT_IDS_CHUNK)
    for i in range(0, len(unique_ids), step):
        chunk = unique_ids[i : i + step]
        query = urllib.parse.urlencode({"ids": ",".join(str(x) for x in chunk)})
        try:
            details = _get_json(f"{ADV_ADVERTS_URL}?{query}", token)
        except Exception:
            continue
        adverts = details.get("adverts") if isinstance(details, dict) else None
        if not isinstance(adverts, list):
            continue
        for advert in adverts:
            if not isinstance(advert, dict):
                continue
            advert_id = _int_value(
                advert.get("id") or advert.get("advertId") or advert.get("advertID")
            )
            nm_settings = advert.get("nm_settings") or advert.get("nmSettings")
            if not advert_id or not isinstance(nm_settings, list):
                continue
            advert_type = _int_value(
                advert.get("type")
                or advert.get("advertType")
                or advert.get("advert_type")
            )
            bid_type = (
                str(advert.get("bid_type") or advert.get("bidType") or "")
                .strip()
                .lower()
            )
            raw_settings = advert.get("settings")
            settings: Dict[str, Any] = (
                raw_settings if isinstance(raw_settings, dict) else {}
            )
            raw_placements = settings.get("placements")
            placements: Dict[str, Any] = (
                raw_placements if isinstance(raw_placements, dict) else {}
            )
            placements_search = bool(placements.get("search"))
            placements_recommendations = bool(placements.get("recommendations"))
            for setting in nm_settings:
                if not isinstance(setting, dict):
                    continue
                nm_id = _int_value(
                    setting.get("nm_id") or setting.get("nmId") or setting.get("nmID")
                )
                if not nm_id:
                    continue
                if nm_filter and nm_id not in nm_filter:
                    continue
                out.append(
                    {
                        "advert_id": advert_id,
                        "nm_id": nm_id,
                        # WB API payload expects camelCase keys.
                        "advertId": advert_id,
                        "nmId": nm_id,
                        "advert_type": advert_type,
                        "bid_type": bid_type,
                        "placements_search": placements_search,
                        "placements_recommendations": placements_recommendations,
                    }
                )
                if ADV_MAX_ITEMS > 0 and len(out) >= ADV_MAX_ITEMS:
                    return out
    return out


def fetch_normquery_stats(
    token: str,
    date_from: str,
    date_to: str,
    items: List[Dict[str, int]],
) -> List[Dict[str, Any]]:
    if not token or not items:
        return []
    payload = {
        "from": date_from,
        "to": date_to,
        "items": items,
    }
    try:
        data = _post_json(ADV_NORMQUERY_STATS_URL, payload, token)
    except Exception:
        return []
    if isinstance(data, list):
        return [row for row in data if isinstance(row, dict)]
    if not isinstance(data, dict):
        return []
    rows = data.get("stats")
    if not isinstance(rows, list):
        # Some API variants use different keys.
        rows = data.get("items")
    if not isinstance(rows, list):
        return []
    return [row for row in rows if isinstance(row, dict)]


def fetch_normquery_daily_stats(
    token: str,
    date_from: str,
    date_to: str,
    items: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Daily normquery stats (v1). Returns items with dailyStats."""
    if not token or not items:
        return []
    payload = {
        "from": str(date_from or "")[:10],
        "to": str(date_to or "")[:10],
        "items": items,
    }
    try:
        data = _post_json(ADV_NORMQUERY_DAILY_STATS_URL, payload, token)
    except Exception:
        return []
    if not isinstance(data, dict):
        return []
    rows = data.get("items")
    if not isinstance(rows, list):
        return []
    return [row for row in rows if isinstance(row, dict)]


def fetch_adv_upd(token: str, date_from: str, date_to: str) -> List[Dict[str, Any]]:
    if not token:
        return []
    frm = str(date_from or "").strip()[:10]
    to = str(date_to or "").strip()[:10]
    if not frm or not to:
        return []
    query = urllib.parse.urlencode({"from": frm, "to": to})
    data = _get_json(f"{ADV_UPD_URL}?{query}", token)
    if not isinstance(data, list):
        return []
    out: List[Dict[str, Any]] = []
    for row in data:
        if not isinstance(row, dict):
            continue
        out.append(
            {
                "upd_num": _int_value(row.get("updNum") or row.get("upd_num")),
                "upd_time": str(row.get("updTime") or row.get("upd_time") or ""),
                "upd_sum": round(
                    _float_value(row.get("updSum") or row.get("upd_sum")), 2
                ),
                "advert_id": _int_value(row.get("advertId") or row.get("advert_id")),
                "camp_name": str(row.get("campName") or row.get("camp_name") or ""),
                "advert_type": _int_value(
                    row.get("advertType") or row.get("advert_type")
                ),
                "payment_type": str(
                    row.get("paymentType") or row.get("payment_type") or ""
                ),
                "advert_status": _int_value(
                    row.get("advertStatus") or row.get("advert_status")
                ),
            }
        )
    return out


def fetch_adv_fullstats(
    token: str,
    advert_ids: List[int],
    begin_date: str,
    end_date: str,
) -> List[Dict[str, Any]]:
    if not token:
        return []
    ids = sorted({int(v) for v in (advert_ids or []) if int(v)})
    if not ids:
        return []
    begin = str(begin_date or "").strip()[:10]
    end = str(end_date or "").strip()[:10]
    if not begin or not end:
        return []

    out: List[Dict[str, Any]] = []
    step = max(1, ADV_FULLSTATS_IDS_CHUNK)
    for i in range(0, len(ids), step):
        chunk = ids[i : i + step]
        query = urllib.parse.urlencode(
            {
                "ids": ",".join(str(x) for x in chunk),
                "beginDate": begin,
                "endDate": end,
            }
        )
        try:
            data = _get_json(f"{ADV_FULLSTATS_URL}?{query}", token)
        except Exception:
            continue
        if not isinstance(data, list):
            continue
        for row in data:
            if isinstance(row, dict):
                out.append(row)
    return out


def build_adv_fullstats_daily_nm_spend(
    rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Flatten /adv/v3/fullstats to advert/day/nm spend rows."""

    agg: Dict[Tuple[int, str, int], float] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        advert_id = _int_value(
            row.get("advertId")
            or row.get("advert_id")
            or row.get("advertID")
            or row.get("id")
        )
        if not advert_id:
            continue
        raw_days = row.get("days")
        days: List[Any] = []
        if isinstance(raw_days, list):
            days = list(raw_days)
        for day_item in days:
            if not isinstance(day_item, dict):
                continue
            day = str(day_item.get("date") or "")[:10]
            if not day:
                continue
            raw_apps = day_item.get("apps")
            apps: List[Any] = raw_apps if isinstance(raw_apps, list) else []
            for app in apps:
                if not isinstance(app, dict):
                    continue
                raw_nms = app.get("nms")
                nms: List[Any] = raw_nms if isinstance(raw_nms, list) else []
                for nm_item in nms:
                    if not isinstance(nm_item, dict):
                        continue
                    nm_id = _int_value(
                        nm_item.get("nmId")
                        or nm_item.get("nmID")
                        or nm_item.get("nm_id")
                    )
                    if not nm_id:
                        continue
                    spend = _float_value(nm_item.get("sum") or nm_item.get("spend"))
                    if abs(spend) <= 1e-9:
                        continue
                    key = (advert_id, day, nm_id)
                    agg[key] = agg.get(key, 0.0) + spend

    out: List[Dict[str, Any]] = []
    for (advert_id, day, nm_id), spend in agg.items():
        out.append(
            {
                "advert_id": advert_id,
                "date": day,
                "nm_id": nm_id,
                "spend": round(spend, 4),
            }
        )
    return out


def _search_products(query: str, max_pages: int) -> List[Dict[str, Any]]:
    text = str(query or "").strip()
    if not text:
        return []
    pages = max(1, max_pages)
    out: List[Dict[str, Any]] = []

    for endpoint in SEARCH_API_URLS:
        out.clear()
        try:
            for page in range(1, pages + 1):
                params = {
                    "ab_testing": "false",
                    "appType": "1",
                    "curr": "rub",
                    "dest": WB_SEARCH_DEST,
                    "hide_dtype": "13",
                    "lang": "ru",
                    "page": str(page),
                    "query": text,
                    "resultset": "catalog",
                    "sort": WB_SEARCH_SORT,
                    "spp": WB_SEARCH_SPP,
                    "suppressSpellcheck": "false",
                }
                url = endpoint + "?" + urllib.parse.urlencode(params)
                payload = _get_public_json(url)
                data = payload.get("data") if isinstance(payload, dict) else None
                products = data.get("products") if isinstance(data, dict) else None
                if not isinstance(products, list) or not products:
                    break
                out.extend([item for item in products if isinstance(item, dict)])
                if len(products) < 100:
                    break
            if out:
                return out
        except Exception:
            continue
    return []


def _search_positions_for_query(
    query: str, nm_ids: List[int], max_pages: int
) -> Dict[int, Tuple[int, int]]:
    text = str(query or "").strip()
    if not text:
        return {}
    targets = sorted({int(x) for x in (nm_ids or []) if int(x) > 0})
    if not targets:
        return {}

    found: Dict[int, Tuple[int, int]] = {}
    pages = max(1, max_pages)
    page_size = 100  # WB search endpoints return up to 100 items per page.

    for endpoint in SEARCH_API_URLS:
        missing = set(targets) - set(found.keys())
        if not missing:
            break
        try:
            for page in range(1, pages + 1):
                params = {
                    "ab_testing": "false",
                    "appType": "1",
                    "curr": "rub",
                    "dest": WB_SEARCH_DEST,
                    "hide_dtype": "13",
                    "lang": "ru",
                    "page": str(page),
                    "query": text,
                    "resultset": "catalog",
                    "sort": WB_SEARCH_SORT,
                    "spp": WB_SEARCH_SPP,
                    "suppressSpellcheck": "false",
                }
                url = endpoint + "?" + urllib.parse.urlencode(params)
                payload = _get_public_json(url)
                products = None
                if isinstance(payload, dict):
                    # v13+: payload.data.products
                    data = payload.get("data")
                    if isinstance(data, dict) and isinstance(
                        data.get("products"), list
                    ):
                        products = data.get("products")
                    # v4: payload.products
                    elif isinstance(payload.get("products"), list):
                        products = payload.get("products")
                    # Some variants include nested keys.
                    elif isinstance(payload.get("search_result"), dict) and isinstance(
                        payload["search_result"].get("products"), list
                    ):
                        products = payload["search_result"].get("products")
                if not isinstance(products, list) or not products:
                    break

                for idx, product in enumerate(products, start=1):
                    if not isinstance(product, dict):
                        continue
                    product_id = _int_value(product.get("id") or product.get("nmId"))
                    if product_id not in missing:
                        continue
                    pos = ((page - 1) * page_size) + idx
                    promo = 0
                    if (
                        _int_value(product.get("advertId")) > 0
                        or _int_value(product.get("adId")) > 0
                        or bool(product.get("isAd"))
                    ):
                        promo = pos
                    found[product_id] = (pos, promo)
                    missing.discard(product_id)
                    if not missing:
                        break

                if not missing:
                    break
                if len(products) < page_size:
                    break
        except Exception:
            continue

    return found


def _search_positions_for_query_dest(
    query: str, nm_ids: List[int], dest: str, max_pages: int
) -> Dict[int, Tuple[int, int]]:
    text = str(query or "").strip()
    if not text:
        return {}
    targets = sorted({int(x) for x in (nm_ids or []) if int(x) > 0})
    if not targets:
        return {}

    dest_norm = str(dest or "").strip() or WB_SEARCH_DEST
    found: Dict[int, Tuple[int, int]] = {}
    pages = max(1, max_pages)
    page_size = 100  # WB search endpoints return up to 100 items per page.

    for endpoint in SEARCH_API_URLS:
        missing = set(targets) - set(found.keys())
        if not missing:
            break
        try:
            for page in range(1, pages + 1):
                params = {
                    "ab_testing": "false",
                    "appType": "1",
                    "curr": "rub",
                    "dest": dest_norm,
                    "hide_dtype": "13",
                    "lang": "ru",
                    "locale": WB_SEARCH_LOCALE,
                    "page": str(page),
                    "query": text,
                    "resultset": "catalog",
                    "sort": WB_SEARCH_SORT,
                    "spp": WB_SEARCH_SPP,
                    "suppressSpellcheck": "false",
                }
                if WB_SEARCH_REGIONS:
                    params["regions"] = WB_SEARCH_REGIONS
                if WB_SEARCH_COUPONS_GEO:
                    params["couponsGeo"] = WB_SEARCH_COUPONS_GEO
                if WB_SEARCH_EMP:
                    params["emp"] = WB_SEARCH_EMP
                if WB_SEARCH_LOCATE:
                    params["locate"] = WB_SEARCH_LOCATE
                if WB_SEARCH_REG:
                    params["reg"] = WB_SEARCH_REG
                if WB_SEARCH_SPP_FIX_GEO:
                    params["sppFixGeo"] = WB_SEARCH_SPP_FIX_GEO
                if WB_SEARCH_PRICE_MARGIN_COEFF:
                    params["pricemarginCoeff"] = WB_SEARCH_PRICE_MARGIN_COEFF
                url = endpoint + "?" + urllib.parse.urlencode(params)
                payload = _get_public_json(url)
                products = None
                if isinstance(payload, dict):
                    # v13+: payload.data.products
                    data = payload.get("data")
                    if isinstance(data, dict) and isinstance(
                        data.get("products"), list
                    ):
                        products = data.get("products")
                    # v4: payload.products
                    elif isinstance(payload.get("products"), list):
                        products = payload.get("products")
                    # Some variants include nested keys.
                    elif isinstance(payload.get("search_result"), dict) and isinstance(
                        payload["search_result"].get("products"), list
                    ):
                        products = payload["search_result"].get("products")
                if not isinstance(products, list) or not products:
                    break

                for idx, product in enumerate(products, start=1):
                    if not isinstance(product, dict):
                        continue
                    product_id = _int_value(product.get("id") or product.get("nmId"))
                    if product_id not in missing:
                        continue
                    pos = ((page - 1) * page_size) + idx
                    promo = 0
                    if (
                        _int_value(product.get("advertId")) > 0
                        or _int_value(product.get("adId")) > 0
                        or bool(product.get("isAd"))
                    ):
                        promo = pos
                    found[product_id] = (pos, promo)
                    missing.discard(product_id)
                    if not missing:
                        break

                if WB_PUBLIC_DELAY_SECONDS > 0:
                    time.sleep(min(max(WB_PUBLIC_DELAY_SECONDS, 0.0), 2.0))

                if not missing:
                    break
                if len(products) < page_size:
                    break
        except Exception:
            continue

    return found


def _aggregate_positions(
    per_dest: Dict[str, Tuple[int, int]], dests: List[str], mode: str
) -> Tuple[int, int]:
    if not dests:
        return (0, 0)
    mode_norm = str(mode or "").strip().lower() or "first"

    if mode_norm in {"first", "primary", "dest_first"}:
        return per_dest.get(str(dests[0]), (0, 0))

    if mode_norm in {"first_nonzero", "first_found"}:
        for dest in dests:
            pos, promo = per_dest.get(str(dest), (0, 0))
            if int(pos or 0) > 0:
                return (int(pos), int(promo or 0))
        return (0, 0)

    found = [
        (dest, pos, promo)
        for dest, (pos, promo) in per_dest.items()
        if int(pos or 0) > 0
    ]
    if not found:
        return (0, 0)

    if mode_norm in {"best"}:
        found.sort(key=lambda x: (x[1], 0 if x[2] <= 0 else x[2], str(x[0])))
        _, pos, promo = found[0]
        return (int(pos), int(promo or 0))

    if mode_norm in {"min"}:
        pos = min(int(x[1]) for x in found)
        promos = [int(x[2]) for x in found if int(x[2] or 0) > 0]
        promo = min(promos) if promos else 0
        return (pos, promo)

    if mode_norm in {"median"}:
        positions = sorted(int(x[1]) for x in found)
        pos = positions[len(positions) // 2]
        promos = sorted(int(x[2]) for x in found if int(x[2] or 0) > 0)
        promo = promos[len(promos) // 2] if promos else 0
        return (pos, promo)

    return per_dest.get(str(dests[0]), (0, 0))


def fetch_search_positions_multi(
    items: List[Dict[str, Any]],
    max_pages: int = WB_SEARCH_MAX_PAGES,
    dests: Optional[List[str]] = None,
    agg_mode: str = "",
) -> Tuple[List[List[Any]], List[Dict[str, Any]]]:
    if not isinstance(items, list) or not items:
        return [], []

    dest_list = [str(x).strip() for x in (dests or WB_SEARCH_DESTS) if str(x).strip()]
    if not dest_list:
        dest_list = [WB_SEARCH_DEST]
    mode = str(agg_mode or WB_SEARCH_POS_AGG or "first").strip().lower() or "first"

    by_query: Dict[str, List[int]] = {}
    ordered: List[Tuple[int, str]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        nm_id = _int_value(
            item.get("itemNumber") or item.get("nm_id") or item.get("nmId")
        )
        query = str(item.get("query") or "").strip()
        if not nm_id or not query:
            continue
        by_query.setdefault(query, []).append(nm_id)
        ordered.append((nm_id, query))

    now_text = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    raw_rows: List[Dict[str, Any]] = []
    rows_2d: List[List[Any]] = []

    # Pre-fetch per (query,dest) mappings.
    maps: Dict[Tuple[str, str], Dict[int, Tuple[int, int]]] = {}
    for query, nm_ids in by_query.items():
        uniq_ids = sorted({int(x) for x in nm_ids if int(x) > 0})
        for dest in dest_list:
            maps[(query, dest)] = _search_positions_for_query_dest(
                query, uniq_ids, dest=dest, max_pages=max_pages
            )

    for nm_id, query in ordered:
        per_dest: Dict[str, Tuple[int, int]] = {}
        for dest in dest_list:
            pos, promo = maps.get((query, dest), {}).get(int(nm_id), (0, 0))
            per_dest[dest] = (int(pos or 0), int(promo or 0))
            raw_rows.append(
                {
                    "captured_at": now_text,
                    "nm_id": int(nm_id),
                    "search_key": query,
                    "dest": str(dest),
                    "position": int(pos or 0),
                    "promo_position": int(promo or 0),
                    "source": "wb_search_public",
                }
            )
        pos_agg, promo_agg = _aggregate_positions(per_dest, dest_list, mode)
        rows_2d.append(
            [now_text, int(nm_id), query, int(pos_agg or 0), int(promo_agg or 0)]
        )

    return rows_2d, raw_rows


def fetch_search_positions(
    items: List[Dict[str, Any]], max_pages: int = WB_SEARCH_MAX_PAGES
) -> List[List[Any]]:
    rows_2d, _raw = fetch_search_positions_multi(items, max_pages=max_pages)
    return rows_2d


def campaign_type_name(advert_type: int) -> str:
    if advert_type in ADV_SEARCH_TYPE_CODES:
        return "search"
    if advert_type in ADV_AUTO_TYPE_CODES:
        return "auto"
    if advert_type <= 0:
        return "unknown"
    return f"type_{advert_type}"


def campaign_bucket(meta: Dict[str, Any]) -> str:
    """Best-effort mapping to a stable bucket used by checklist fields.

    WB advertising types evolved over time (e.g. type=9 can be manual or unified bids).
    Use extra metadata when available, with env overrides.
    """

    advert_type = _int_value(
        meta.get("advert_type") or meta.get("advertType") or meta.get("type")
    )
    bid_type = str(meta.get("bid_type") or meta.get("bidType") or "").strip().lower()
    placements_search = (
        bool(meta.get("placements_search")) if "placements_search" in meta else None
    )
    placements_reco = (
        bool(meta.get("placements_recommendations"))
        if "placements_recommendations" in meta
        else None
    )

    if bid_type:
        if bid_type in ADV_AUTO_BID_TYPES:
            return "auto"
        if bid_type in ADV_SEARCH_BID_TYPES:
            return "search"

    # Placements can hint to bucket even when bidType is not present.
    if placements_search is True and placements_reco is not True:
        return "search"
    if placements_reco is True and placements_search is not True:
        return "auto"

    return campaign_type_name(advert_type)


def build_adv_normquery_rows(
    token: str,
    date_from: str,
    date_to: str,
    nm_ids: Optional[List[int]] = None,
    min_views: int = 0,
) -> List[Dict[str, Any]]:
    items = fetch_advert_items(token, nm_ids=nm_ids)
    if not items:
        return []
    blocks = fetch_normquery_stats(token, date_from, date_to, items)
    if not blocks:
        return []

    meta_map: Dict[Tuple[int, int], Dict[str, Any]] = {}
    for item in items:
        advert_id = _int_value(item.get("advert_id"))
        nm_id = _int_value(item.get("nm_id"))
        if not advert_id or not nm_id:
            continue
        meta_map[(advert_id, nm_id)] = item

    out: List[Dict[str, Any]] = []
    for block in blocks:
        if not isinstance(block, dict):
            continue
        advert_id = _int_value(
            block.get("advert_id") or block.get("advertId") or block.get("advertID")
        )
        nm_id = _int_value(block.get("nm_id") or block.get("nmId") or block.get("nmID"))
        if not advert_id or not nm_id:
            continue
        meta = meta_map.get((advert_id, nm_id), {})
        bucket_meta = dict(meta) if isinstance(meta, dict) else {}
        advert_type = _int_value(bucket_meta.get("advert_type"))
        bucket_meta["advert_type"] = advert_type
        camp_type = campaign_bucket(bucket_meta)
        stats = block.get("stats")
        if not isinstance(stats, list):
            continue
        for cluster in stats:
            if not isinstance(cluster, dict):
                continue
            views = _int_value(cluster.get("views"))
            if views < max(0, min_views):
                continue
            clicks = _int_value(cluster.get("clicks"))
            orders = _int_value(cluster.get("orders"))
            spend = max(_float_value(cluster.get("spend") or cluster.get("sum")), 0.0)
            out.append(
                {
                    "date_from": date_from[:10],
                    "date_to": date_to[:10],
                    "advert_id": advert_id,
                    "advert_type": advert_type,
                    "campaign_type": camp_type,
                    "nm_id": nm_id,
                    "search_key": str(
                        cluster.get("norm_query") or cluster.get("normQuery") or ""
                    ).strip(),
                    "views": views,
                    "clicks": clicks,
                    "atbs": _int_value(cluster.get("atbs")),
                    "orders": orders,
                    "spend": round(spend, 2),
                    "ctr": round(_float_value(cluster.get("ctr")), 4),
                    "cpc": round(_float_value(cluster.get("cpc")), 2),
                    "cpm": round(_float_value(cluster.get("cpm")), 2),
                    "avg_position": round(
                        _float_value(cluster.get("avg_pos") or cluster.get("avgPos")), 2
                    ),
                }
            )
    return out


def build_search_items_from_adv_rows(
    rows: List[Dict[str, Any]],
    max_keys_per_nm: int = 5,
    min_views: int = 10,
) -> List[Dict[str, Any]]:
    grouped: Dict[Tuple[int, str], Dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        nm_id = _int_value(row.get("nm_id"))
        query = str(row.get("search_key") or "").strip()
        if not nm_id or not query:
            continue
        key = (nm_id, query)
        item = grouped.setdefault(
            key,
            {
                "nm_id": nm_id,
                "query": query,
                "views": 0,
                "clicks": 0,
                "orders": 0,
                "spend": 0.0,
            },
        )
        item["views"] += _int_value(row.get("views"))
        item["clicks"] += _int_value(row.get("clicks"))
        item["orders"] += _int_value(row.get("orders"))
        item["spend"] += _float_value(row.get("spend"))

    by_nm: Dict[int, List[Dict[str, Any]]] = {}
    for item in grouped.values():
        if item["views"] < max(0, min_views):
            continue
        by_nm.setdefault(item["nm_id"], []).append(item)

    out: List[Dict[str, Any]] = []
    for nm_id, nm_items in by_nm.items():
        nm_items.sort(
            key=lambda it: (it["views"], it["orders"], it["spend"]), reverse=True
        )
        for it in nm_items[: max(1, max_keys_per_nm)]:
            out.append({"itemNumber": nm_id, "query": it["query"]})
    return out
