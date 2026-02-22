import json
from datetime import datetime, timedelta
import os
import threading
import time
import urllib.parse
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, List, Optional, Set, Tuple

from datasets import (
    SUPPORTED_DATA_DATASETS,
    SUPPORTED_UPDATE_DATASETS,
    SUPPORTED_UPLOAD_DATASETS,
    handle_checklist,
    handle_data,
    handle_update,
    handle_upload,
    parse_dataset_payload,
)
from storage import Storage
from wb import token_sid
from wb_client import (
    build_adv_normquery_rows,
    build_search_items_from_adv_rows,
    fetch_adv_upd,
    fetch_search_positions,
    fetch_search_positions_multi,
)
from collector import run_collection

try:
    from google_sheets import GoogleSheetsClient
except Exception:  # pragma: no cover
    GoogleSheetsClient = None  # type: ignore


HOST = os.environ.get("BTLZ_HOST", "127.0.0.1")
PORT = int(os.environ.get("BTLZ_PORT", "8080"))
DB_PATH = os.environ.get(
    "BTLZ_DB_PATH", os.path.join(os.path.dirname(__file__), "data", "btlz.db")
)
API_KEY = os.environ.get("BTLZ_API_KEY", "")
TECH_SHEETS_LIST = [
    item.strip()
    for item in os.environ.get(
        "BTLZ_TECH_SHEETS_LIST",
        "ce,checklist,unit_log,mp_stats,mp_conv,config,today,cards,checklist_cross,rnp_tech",
    ).split(",")
    if item.strip()
]
TECH_SHEETS_EDITORS = [
    item.strip()
    for item in os.environ.get("BTLZ_TECH_SHEETS_EDITORS", "").split(",")
    if item.strip()
]
BEARER_TOKENS_RAW = str(os.environ.get("BTLZ_BEARER_TOKENS_JSON", "") or "").strip()
RATE_LIMIT_WINDOW_SECONDS = max(
    int(os.environ.get("BTLZ_RATE_LIMIT_WINDOW_SECONDS", "60") or 60),
    1,
)
RATE_LIMIT_ADMIN_PER_WINDOW = max(
    int(os.environ.get("BTLZ_RATE_LIMIT_ADMIN_PER_WINDOW", "120") or 120),
    1,
)
RATE_LIMIT_ACTIONS_PER_WINDOW = max(
    int(os.environ.get("BTLZ_RATE_LIMIT_ACTIONS_PER_WINDOW", "240") or 240),
    1,
)
RATE_LIMIT_DATA_PER_WINDOW = max(
    int(os.environ.get("BTLZ_RATE_LIMIT_DATA_PER_WINDOW", "1200") or 1200),
    1,
)
COLLECTOR_LOCK_TTL_SECONDS = max(
    int(os.environ.get("BTLZ_COLLECTOR_LOCK_TTL_SECONDS", "1800") or 1800),
    60,
)
AUDIT_ENABLED = str(
    os.environ.get("BTLZ_AUDIT_ENABLED", "1") or ""
).strip().lower() in {
    "1",
    "true",
    "yes",
}

_METRICS_LOCK = threading.Lock()
_METRICS: Dict[str, int] = {
    "http_requests_total": 0,
    "http_auth_failures_total": 0,
    "http_forbidden_total": 0,
    "http_rate_limited_total": 0,
    "http_onboarding_success_total": 0,
    "http_onboarding_failures_total": 0,
    "http_collector_lock_conflicts_total": 0,
}


def _metric_inc(name: str, value: int = 1) -> None:
    key = str(name or "").strip()
    if not key:
        return
    delta = int(value or 0)
    if delta == 0:
        return
    with _METRICS_LOCK:
        _METRICS[key] = int(_METRICS.get(key, 0)) + delta


def _metrics_snapshot() -> Dict[str, int]:
    with _METRICS_LOCK:
        return {k: int(v) for k, v in _METRICS.items()}


def _load_bearer_tokens(raw: str) -> Dict[str, Dict[str, Any]]:
    text = str(raw or "").strip()
    if not text:
        return {}
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        return {}
    if not isinstance(data, dict):
        return {}
    out: Dict[str, Dict[str, Any]] = {}
    for token, value in data.items():
        token_text = str(token or "").strip()
        if not token_text:
            continue
        role = "client"
        actor = ""
        spreadsheet_ids: List[str] = []
        if isinstance(value, str):
            role = str(value or "client").strip().lower() or "client"
        elif isinstance(value, dict):
            role = str(value.get("role") or "client").strip().lower() or "client"
            actor = str(value.get("actor") or "").strip()
            raw_spreadsheets = value.get("spreadsheets")
            if isinstance(raw_spreadsheets, str):
                spreadsheet_ids = [
                    item.strip()
                    for item in raw_spreadsheets.split(",")
                    if item and item.strip()
                ]
            elif isinstance(raw_spreadsheets, (list, tuple, set)):
                spreadsheet_ids = [
                    str(item or "").strip()
                    for item in raw_spreadsheets
                    if str(item or "").strip()
                ]
        out[token_text] = {
            "role": role,
            "actor": actor,
            "spreadsheets": spreadsheet_ids,
        }
    return out


BEARER_TOKENS = _load_bearer_tokens(BEARER_TOKENS_RAW)
ADMIN_ROLES: Set[str] = {"admin", "owner"}
OPERATOR_ROLES: Set[str] = {"admin", "owner", "operator"}
CLIENT_ROLES: Set[str] = {"admin", "owner", "operator", "client"}


@dataclass
class AuthContext:
    ok: bool
    actor: str
    role: str
    spreadsheet_ids: Set[str]
    message: str


def _json_bytes(payload: Any) -> bytes:
    return json.dumps(payload, ensure_ascii=False).encode("utf-8")


def _normalize_path(path: str) -> str:
    clean = path.split("?", 1)[0]
    if clean.startswith("/api/"):
        return clean[4:]
    if clean == "/api":
        return "/"
    return clean


class ApiHandler(BaseHTTPRequestHandler):
    storage = Storage(DB_PATH)
    _rate_lock = threading.Lock()
    _rate_hits: Dict[str, List[float]] = {}

    def _send(self, code: int, payload: Any) -> None:
        body = _json_bytes(payload)
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

        audit_ctx = getattr(self, "_audit_ctx", None)
        if audit_ctx and AUDIT_ENABLED:
            try:
                self.storage.add_audit_event(
                    event_type=str(audit_ctx.get("event_type") or "http_request"),
                    actor=str(audit_ctx.get("actor") or ""),
                    role=str(audit_ctx.get("role") or ""),
                    method=str(audit_ctx.get("method") or ""),
                    path=str(audit_ctx.get("path") or ""),
                    status_code=int(code),
                    ip=str(audit_ctx.get("ip") or ""),
                    payload=audit_ctx.get("payload") or {},
                )
            except Exception as exc:
                print(f"[backend] audit write failed: {exc}")
            finally:
                self._audit_ctx = None

    def _read_json(self) -> Dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length > 0 else b"{}"
        if not raw:
            return {}
        try:
            data = json.loads(raw.decode("utf-8"))
            if isinstance(data, dict):
                return data
        except json.JSONDecodeError:
            pass
        raise ValueError("invalid JSON body")

    def _extract_bearer_token(self) -> str:
        auth = str(self.headers.get("Authorization", "") or "")
        if not auth:
            return ""
        parts = auth.split(" ", 1)
        if len(parts) != 2:
            return ""
        if parts[0].strip().lower() != "bearer":
            return ""
        return parts[1].strip()

    def _auth_context(self) -> AuthContext:
        bearer = self._extract_bearer_token()
        x_api_key = str(self.headers.get("X-Api-Key", "") or "").strip()

        if not API_KEY and not BEARER_TOKENS:
            return AuthContext(
                ok=False,
                actor="",
                role="",
                spreadsheet_ids=set(),
                message="authentication is not configured",
            )

        if API_KEY:
            if x_api_key == API_KEY or (bearer and bearer == API_KEY):
                return AuthContext(
                    ok=True,
                    actor="api_key",
                    role="admin",
                    spreadsheet_ids={"*"},
                    message="",
                )

        if bearer and bearer in BEARER_TOKENS:
            info = BEARER_TOKENS.get(bearer) or {}
            role = str(info.get("role") or "client").strip().lower() or "client"
            actor = str(info.get("actor") or "").strip() or "bearer"
            spreadsheet_ids_raw = info.get("spreadsheets")
            spreadsheet_ids: Set[str] = set()
            if isinstance(spreadsheet_ids_raw, (list, tuple, set)):
                spreadsheet_ids = {
                    str(item or "").strip()
                    for item in spreadsheet_ids_raw
                    if str(item or "").strip()
                }
            return AuthContext(
                ok=True,
                actor=actor,
                role=role,
                spreadsheet_ids=spreadsheet_ids,
                message="",
            )

        if API_KEY:
            return AuthContext(
                ok=False,
                actor="",
                role="",
                spreadsheet_ids=set(),
                message="invalid API key or bearer token",
            )
        return AuthContext(
            ok=False,
            actor="",
            role="",
            spreadsheet_ids=set(),
            message="missing or invalid bearer token",
        )

    def _spreadsheet_id_from_path(self, path: str) -> str:
        parts = [part for part in path.split("/") if part]
        if len(parts) >= 2 and parts[0] == "ss":
            return str(parts[1] or "").strip()
        return ""

    def _spreadsheet_id_from_payload(self, payload: Dict[str, Any]) -> str:
        if not isinstance(payload, dict):
            return ""
        spreadsheet_id = payload.get("spreadsheet_id") or payload.get("ssId")
        return str(spreadsheet_id or "").strip()

    def _check_spreadsheet_scope(
        self,
        path: str,
        payload: Dict[str, Any],
        auth: AuthContext,
    ) -> Tuple[bool, str]:
        if not path.startswith("/ss/"):
            return True, ""

        spreadsheet_id = self._spreadsheet_id_from_payload(payload)
        if not spreadsheet_id:
            spreadsheet_id = self._spreadsheet_id_from_path(path)
        if not spreadsheet_id:
            return False, "spreadsheet_id is required"

        if not self.storage.is_spreadsheet_active(spreadsheet_id):
            return False, "spreadsheet is inactive or not registered"

        if auth.role in OPERATOR_ROLES:
            return True, ""
        if "*" in auth.spreadsheet_ids:
            return True, ""
        if not auth.spreadsheet_ids:
            return False, "forbidden"
        if spreadsheet_id in auth.spreadsheet_ids:
            return True, ""
        return False, "forbidden"

    def _requires_roles(self, path: str) -> Optional[Set[str]]:
        if path.startswith("/admin/"):
            return OPERATOR_ROLES
        if path == "/actions":
            return OPERATOR_ROLES
        if path.startswith("/ss/"):
            return CLIENT_ROLES
        return None

    def _check_access(self, path: str, auth: AuthContext) -> Tuple[bool, str]:
        required = self._requires_roles(path)
        if required is None:
            return True, ""
        if auth.role in required:
            return True, ""
        return False, "forbidden"

    def _rate_limit_value(self, path: str) -> int:
        if path.startswith("/admin/"):
            return RATE_LIMIT_ADMIN_PER_WINDOW
        if path == "/actions":
            return RATE_LIMIT_ACTIONS_PER_WINDOW
        return RATE_LIMIT_DATA_PER_WINDOW

    def _check_rate_limit(self, path: str, auth: AuthContext) -> Tuple[bool, int]:
        limit = self._rate_limit_value(path)
        now = time.time()
        ip = str(self.client_address[0] if self.client_address else "")
        bucket_key = f"{path}|{ip}|{auth.role}|{auth.actor}"
        with self._rate_lock:
            hits = self._rate_hits.get(bucket_key, [])
            cutoff = now - float(RATE_LIMIT_WINDOW_SECONDS)
            hits = [x for x in hits if x >= cutoff]
            if len(hits) >= limit:
                self._rate_hits[bucket_key] = hits
                return False, limit
            hits.append(now)
            self._rate_hits[bucket_key] = hits
        return True, limit

    def _sanitize_payload(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(payload, dict):
            return {}

        secret_keys = {"token", "password", "api_key", "secret"}

        def _walk(value: Any) -> Any:
            if isinstance(value, dict):
                out: Dict[str, Any] = {}
                for key, item in value.items():
                    key_text = str(key or "")
                    if key_text.lower() in secret_keys:
                        out[key_text] = "***"
                    else:
                        out[key_text] = _walk(item)
                return out
            if isinstance(value, list):
                return [_walk(item) for item in value]
            return value

        data = _walk(payload)
        return data if isinstance(data, dict) else {}

    def _set_audit_context(
        self, path: str, auth: AuthContext, payload: Dict[str, Any]
    ) -> None:
        self._audit_ctx = {
            "event_type": "http_request",
            "actor": auth.actor,
            "role": auth.role,
            "method": self.command,
            "path": path,
            "ip": str(self.client_address[0] if self.client_address else ""),
            "payload": self._sanitize_payload(payload),
        }

    def log_message(self, format: str, *args: Any) -> None:
        print("[backend]", format % args)

    def do_GET(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        path = _normalize_path(parsed.path)
        _metric_inc("http_requests_total")
        if path == "/health":
            self._send(HTTPStatus.OK, {"success": True, "status": "ok"})
            return

        auth = self._auth_context()
        if not auth.ok:
            _metric_inc("http_auth_failures_total")
            self._send(
                HTTPStatus.UNAUTHORIZED,
                {"success": False, "message": auth.message},
            )
            return
        access_ok, access_message = self._check_access(path, auth)
        if not access_ok:
            _metric_inc("http_forbidden_total")
            self._send(
                HTTPStatus.FORBIDDEN,
                {"success": False, "message": access_message},
            )
            return
        scope_ok, scope_message = self._check_spreadsheet_scope(path, {}, auth)
        if not scope_ok:
            is_bad_request = "required" in str(scope_message or "").lower()
            if not is_bad_request:
                _metric_inc("http_forbidden_total")
            self._send(
                HTTPStatus.BAD_REQUEST if is_bad_request else HTTPStatus.FORBIDDEN,
                {"success": False, "message": scope_message},
            )
            return
        rate_ok, _ = self._check_rate_limit(path, auth)
        if not rate_ok:
            _metric_inc("http_rate_limited_total")
            self._send(
                HTTPStatus.TOO_MANY_REQUESTS,
                {"success": False, "message": "rate limit exceeded"},
            )
            return
        self._set_audit_context(path, auth, {})

        if path == "/admin/metrics":
            self._send(
                HTTPStatus.OK,
                {
                    "success": True,
                    "result": _metrics_snapshot(),
                },
            )
            return

        parts = [part for part in path.split("/") if part]
        if (
            len(parts) >= 5
            and parts[0] == "ss"
            and parts[2] == "dataset"
            and parts[3] == "wb"
            and parts[4] == "checklist"
        ):
            spreadsheet_id = str(parts[1])
            query = urllib.parse.parse_qs(parsed.query)
            nm_ids_raw = []
            for item in query.get("nm_ids", []):
                nm_ids_raw.extend([chunk.strip() for chunk in str(item).split(",")])
            nm_ids = []
            for item in nm_ids_raw:
                try:
                    val = int(float(item))
                except (TypeError, ValueError):
                    continue
                if val:
                    nm_ids.append(val)
            date_from = str(query.get("date_from", [""])[0] or "")
            result = handle_checklist(
                self.storage,
                spreadsheet_id,
                nm_ids,
                date_from,
            )
            self._send(HTTPStatus.OK, result)
            return
        self._send(HTTPStatus.NOT_FOUND, {"success": False, "message": "not found"})

    def do_POST(self) -> None:
        path = _normalize_path(self.path)
        _metric_inc("http_requests_total")

        auth = self._auth_context()
        if not auth.ok:
            _metric_inc("http_auth_failures_total")
            self._send(
                HTTPStatus.UNAUTHORIZED,
                {"success": False, "message": auth.message},
            )
            return

        access_ok, access_message = self._check_access(path, auth)
        if not access_ok:
            _metric_inc("http_forbidden_total")
            self._send(
                HTTPStatus.FORBIDDEN,
                {"success": False, "message": access_message},
            )
            return

        rate_ok, _ = self._check_rate_limit(path, auth)
        if not rate_ok:
            _metric_inc("http_rate_limited_total")
            self._send(
                HTTPStatus.TOO_MANY_REQUESTS,
                {
                    "success": False,
                    "message": "rate limit exceeded",
                },
            )
            return

        try:
            payload = self._read_json()
        except ValueError as exc:
            self._send(HTTPStatus.BAD_REQUEST, {"success": False, "message": str(exc)})
            return

        scope_ok, scope_message = self._check_spreadsheet_scope(path, payload, auth)
        if not scope_ok:
            is_bad_request = "required" in str(scope_message or "").lower()
            if not is_bad_request:
                _metric_inc("http_forbidden_total")
            self._send(
                HTTPStatus.BAD_REQUEST if is_bad_request else HTTPStatus.FORBIDDEN,
                {"success": False, "message": scope_message},
            )
            return

        self._set_audit_context(path, auth, payload)

        if path == "/health":
            self._send(HTTPStatus.OK, {"success": True, "status": "ok"})
            return

        if path == "/admin/spreadsheets/register":
            spreadsheet_id = payload.get("spreadsheet_id") or payload.get("ssId")
            owner_email = payload.get("owner_email")
            if not spreadsheet_id:
                self._send(
                    HTTPStatus.BAD_REQUEST,
                    {"success": False, "message": "spreadsheet_id is required"},
                )
                return
            self.storage.register_spreadsheet(
                str(spreadsheet_id), owner_email=owner_email
            )
            self._send(
                HTTPStatus.OK,
                {"success": True, "result": {"spreadsheet_id": spreadsheet_id}},
            )
            return

        if path == "/admin/wb/tokens/add":
            spreadsheet_id = payload.get("spreadsheet_id") or payload.get("ssId")
            token = payload.get("token")
            if not spreadsheet_id or not token:
                _metric_inc("http_onboarding_failures_total")
                self._send(
                    HTTPStatus.BAD_REQUEST,
                    {
                        "success": False,
                        "message": "spreadsheet_id and token are required",
                    },
                )
                return

            try:
                sid = token_sid(str(token))
                self.storage.register_spreadsheet(
                    str(spreadsheet_id), owner_email=payload.get("owner_email")
                )
                self.storage.add_wb_token(str(spreadsheet_id), str(token), sid)
            except Exception as exc:
                _metric_inc("http_onboarding_failures_total")
                self._send(
                    HTTPStatus.INTERNAL_SERVER_ERROR,
                    {
                        "success": False,
                        "message": f"failed to add token: {exc}",
                    },
                )
                return

            _metric_inc("http_onboarding_success_total")
            self._send(
                HTTPStatus.OK,
                {
                    "success": True,
                    "result": {
                        "spreadsheet_id": spreadsheet_id,
                        "sid": sid,
                    },
                },
            )
            return

        if path == "/admin/wb/tokens/list":
            spreadsheet_id = payload.get("spreadsheet_id") or payload.get("ssId")
            if not spreadsheet_id:
                self._send(
                    HTTPStatus.BAD_REQUEST,
                    {"success": False, "message": "spreadsheet_id is required"},
                )
                return
            items = self.storage.list_wb_tokens(str(spreadsheet_id))
            result = [
                {
                    "sid": item.sid,
                    "is_active": item.is_active,
                    "token_suffix": item.token[-6:] if item.token else "",
                }
                for item in items
            ]
            self._send(HTTPStatus.OK, {"success": True, "result": result})
            return

        if path == "/ss/wb/token/get":
            spreadsheet_id = payload.get("spreadsheet_id") or payload.get("ssId")
            if not spreadsheet_id:
                self._send(
                    HTTPStatus.BAD_REQUEST,
                    {"success": False, "message": "spreadsheet_id is required"},
                )
                return
            items = self.storage.list_wb_tokens(str(spreadsheet_id))
            if auth.role in OPERATOR_ROLES:
                result = [
                    {
                        "token": item.token,
                        "sid": item.sid,
                        "analytics": True,
                    }
                    for item in items
                    if item.is_active
                ]
            else:
                result = [
                    {
                        "sid": item.sid,
                        "analytics": True,
                        "token_suffix": item.token[-6:] if item.token else "",
                    }
                    for item in items
                    if item.is_active
                ]
            self._send(HTTPStatus.OK, {"result": result})
            return

        if path == "/ss/datasets/data":
            try:
                spreadsheet_id, values, dataset_name = parse_dataset_payload(payload)
            except ValueError as exc:
                self._send(
                    HTTPStatus.BAD_REQUEST, {"success": False, "message": str(exc)}
                )
                return

            result = handle_data(self.storage, spreadsheet_id, dataset_name, values)
            if dataset_name not in SUPPORTED_DATA_DATASETS:
                self._send(
                    HTTPStatus.OK,
                    {
                        "success": True,
                        "warning": "dataset not implemented",
                        "result": result,
                    },
                )
                return
            self._send(HTTPStatus.OK, result)
            return

        if path == "/ss/datasets/upload":
            try:
                spreadsheet_id, values, dataset_name = parse_dataset_payload(payload)
            except ValueError as exc:
                self._send(
                    HTTPStatus.BAD_REQUEST, {"success": False, "message": str(exc)}
                )
                return

            result = handle_upload(self.storage, spreadsheet_id, dataset_name, values)
            if dataset_name not in SUPPORTED_UPLOAD_DATASETS:
                self._send(
                    HTTPStatus.OK,
                    {
                        "success": True,
                        "warning": "dataset not implemented",
                        "result": result,
                    },
                )
                return
            self._send(HTTPStatus.OK, result)
            return

        if path == "/ss/datasets/update":
            spreadsheet_id_raw = payload.get("spreadsheet_id") or payload.get("ssId")
            dataset_payload = payload.get("dataset")
            if dataset_payload is None:
                if not spreadsheet_id_raw:
                    self._send(
                        HTTPStatus.BAD_REQUEST,
                        {
                            "success": False,
                            "message": "spreadsheet_id or ssId is required",
                        },
                    )
                    return
                self._send(
                    HTTPStatus.OK,
                    {
                        "success": True,
                        "message": "accepted",
                        "spreadsheet_id": str(spreadsheet_id_raw),
                        "dataset": None,
                        "values": {},
                    },
                )
                return
            try:
                spreadsheet_id, values, dataset_name = parse_dataset_payload(payload)
            except ValueError as exc:
                self._send(
                    HTTPStatus.BAD_REQUEST, {"success": False, "message": str(exc)}
                )
                return

            result = handle_update(self.storage, spreadsheet_id, dataset_name, values)
            if dataset_name not in SUPPORTED_UPDATE_DATASETS:
                result["warning"] = "dataset not implemented"
            self._send(HTTPStatus.OK, result)
            return

        if path == "/actions":
            self._send(HTTPStatus.OK, self._handle_action(payload, auth))
            return

        self._send(
            HTTPStatus.NOT_FOUND, {"success": False, "message": f"unknown path: {path}"}
        )

    def _calculate_group_supply(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        groups = payload.get("groups")
        if not isinstance(groups, dict):
            groups = {}
        sku = payload.get("sku")
        row_count = len(sku) if isinstance(sku, list) else 0
        if row_count <= 0:
            return {"success": True, "result": "Нет данных для расчета"}

        redemption = payload.get("redemption_percentage")
        redemption_list = redemption if isinstance(redemption, list) else []
        consider_purchase_rate = bool(payload.get("considerPurchaseRate"))
        report_days = float(payload.get("reportDays") or 0)
        sort_days = float(payload.get("sortCalculationDays") or 0)
        prod_delivery_days = float(payload.get("productionAndDeliveryTime") or 0)
        consider_delivery_time = bool(payload.get("considerDeliveryTime"))
        horizon = max(report_days + sort_days, 1.0)
        if consider_delivery_time:
            horizon += max(prod_delivery_days, 0.0)

        rounding_precision = int(float(payload.get("roundingPrecision") or 0))
        calculated_groups = 0
        total_nonzero = 0

        for _, group_data in groups.items():
            if not isinstance(group_data, dict):
                continue
            sales = group_data.get("sales")
            remains = group_data.get("remains")
            delivery = group_data.get("delivery")
            if not isinstance(sales, list) or not isinstance(remains, list):
                continue
            delivery_list = delivery if isinstance(delivery, list) else []
            supply: List[float] = []

            rows = min(row_count, len(sales), len(remains))
            for idx in range(rows):
                sales_value = float(sales[idx] or 0)
                remains_value = float(remains[idx] or 0)
                delivery_value = (
                    float(delivery_list[idx] or 0) if idx < len(delivery_list) else 0.0
                )
                target = max((sales_value / max(report_days, 1.0)) * horizon, 0.0)
                need = max(target - remains_value - delivery_value, 0.0)
                if consider_purchase_rate and idx < len(redemption_list):
                    buyout = float(redemption_list[idx] or 0)
                    if buyout > 0:
                        need = need / max(min(buyout / 100.0, 1.0), 0.01)
                value = round(need, max(0, rounding_precision))
                if value > 0:
                    total_nonzero += 1
                supply.append(value)

            group_data["supply"] = supply
            calculated_groups += 1

        return {
            "success": True,
            "result": (
                f"Расчет выполнен: {calculated_groups} групп, "
                f"{row_count} SKU, ненулевых поставок {total_nonzero}"
            ),
            "data": {"groups": groups},
        }

    def _handle_action(
        self,
        payload: Dict[str, Any],
        auth: Optional[AuthContext] = None,
    ) -> Dict[str, Any]:
        action = payload.get("action") or payload.get("act")
        if not action:
            return {"success": False, "message": "action/act is required"}

        if action == "get_tech_sheets_list":
            return {"success": True, "result": TECH_SHEETS_LIST}

        if action == "get_tech_sheets_editors":
            return {"success": True, "result": TECH_SHEETS_EDITORS}

        if action == "processGroupCalculations":
            return self._calculate_group_supply(payload)

        if action in {"search/keys/upsert", "search_keys/upsert"}:
            ss_id = payload.get("ssId") or payload.get("spreadsheet_id")
            if not ss_id:
                return {"success": False, "message": "ssId/spreadsheet_id is required"}
            raw_items = payload.get("items") or payload.get("data") or []
            if not isinstance(raw_items, list):
                raw_items = []
            items: List[Dict[str, Any]] = []
            for item in raw_items:
                if not isinstance(item, dict):
                    continue
                try:
                    nm_id = int(
                        float(
                            item.get("nm_id")
                            or item.get("itemNumber")
                            or item.get("nmId")
                            or 0
                        )
                    )
                except (TypeError, ValueError):
                    continue
                query = str(item.get("search_key") or item.get("query") or "").strip()
                if not nm_id or not query:
                    continue
                items.append(
                    {
                        "nm_id": nm_id,
                        "search_key": query,
                        "search_key_id": f"{nm_id}__{query}",
                    }
                )
            stored = self.storage.upsert_search_keys(str(ss_id), items, source="manual")
            return {"success": True, "action": action, "stored": stored, "result": "ok"}

        if action in {"search/positions/collect", "search_positions/collect"}:
            ss_id = payload.get("ssId") or payload.get("spreadsheet_id")
            if not ss_id:
                return {"success": False, "message": "ssId/spreadsheet_id is required"}
            nm_ids = payload.get("nm_ids") or payload.get("nmIds") or []
            nm_ids_int: List[int] = []
            if isinstance(nm_ids, list):
                for item in nm_ids:
                    try:
                        val = int(float(item))
                    except (TypeError, ValueError):
                        continue
                    if val:
                        nm_ids_int.append(val)
            keys = self.storage.list_search_keys(
                str(ss_id), nm_ids=nm_ids_int if nm_ids_int else None
            )
            items = [
                {
                    "itemNumber": int(row.get("nm_id") or 0),
                    "query": str(row.get("search_key") or ""),
                }
                for row in keys
                if int(row.get("nm_id") or 0)
                and str(row.get("search_key") or "").strip()
            ]
            rows, geo_rows = fetch_search_positions_multi(items)
            day = datetime.utcnow().strftime("%Y-%m-%d")
            prepared: List[Dict[str, Any]] = []
            for row in rows:
                if not isinstance(row, list) or len(row) < 5:
                    continue
                try:
                    nm_id = int(float(row[1] or 0))
                except (TypeError, ValueError):
                    continue
                query = str(row[2] or "").strip()
                if not nm_id or not query:
                    continue
                prepared.append(
                    {
                        "date": day,
                        "captured_at": str(row[0] or ""),
                        "nm_id": nm_id,
                        "search_key": query,
                        "position": int(float(row[3] or 0))
                        if row[3] is not None
                        else 0,
                        "promo_position": int(float(row[4] or 0))
                        if row[4] is not None
                        else 0,
                        "source": "wb_search",
                    }
                )
            stored = self.storage.upsert_daily_positions(str(ss_id), day, prepared)
            stored_geo = 0
            if geo_rows:
                stored_geo = self.storage.upsert_daily_positions_geo(
                    str(ss_id), day, geo_rows
                )
            return {
                "success": True,
                "action": action,
                "result": f"stored {stored} positions",
                "stored": stored,
                "stored_geo": stored_geo,
                "keys": len(items),
            }

        if action == "search/positions":
            raw_items = payload.get("items")
            items: List[Dict[str, Any]] = (
                raw_items if isinstance(raw_items, list) else []
            )
            rows, geo_rows = fetch_search_positions_multi(items)
            ss_id = payload.get("ssId") or payload.get("spreadsheet_id")
            stored = 0
            if ss_id:
                day = datetime.utcnow().strftime("%Y-%m-%d")
                prepared: List[Dict[str, Any]] = []
                for row in rows:
                    if not isinstance(row, list) or len(row) < 5:
                        continue
                    try:
                        nm_id = int(float(row[1] or 0))
                    except (TypeError, ValueError):
                        continue
                    query = str(row[2] or "").strip()
                    if not nm_id or not query:
                        continue
                    try:
                        position = int(float(row[3] or 0))
                    except (TypeError, ValueError):
                        position = 0
                    try:
                        promo_position = int(float(row[4] or 0))
                    except (TypeError, ValueError):
                        promo_position = 0
                    prepared.append(
                        {
                            "date": day,
                            "captured_at": str(row[0] or ""),
                            "nm_id": nm_id,
                            "search_key": query,
                            "position": position,
                            "promo_position": promo_position,
                            "source": "wb_search",
                        }
                    )
                stored = self.storage.upsert_daily_positions(str(ss_id), day, prepared)
                if geo_rows:
                    try:
                        self.storage.upsert_daily_positions_geo(
                            str(ss_id), day, geo_rows
                        )
                    except Exception:
                        pass
            return {
                "success": True,
                "searchPositions": rows,
                "result": rows,
                "stored": stored,
            }

        if action == "wb/adv/v1/upd/insert":
            fetch_data = payload.get("fetchData") or payload.get("fetch_data") or []
            if not isinstance(fetch_data, list):
                fetch_data = []

            data_items: List[Dict[str, Any]] = []
            for item in fetch_data:
                if not isinstance(item, dict):
                    continue
                token = str(item.get("token") or "").strip()
                date_from = str(item.get("dateFrom") or item.get("date_from") or "")
                date_to = str(item.get("dateTo") or item.get("date_to") or "")
                if not token or not date_from or not date_to:
                    continue
                try:
                    rows = fetch_adv_upd(token, date_from, date_to)
                except Exception:
                    rows = []
                sid = token_sid(token)
                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    if sid:
                        row["sid"] = sid
                    data_items.append(row)

            # Try to write to the sheet if Google SA is configured; otherwise return data.
            ss_id = payload.get("ssId") or payload.get("spreadsheet_id")
            sheet_name = (
                payload.get("sheetName") or payload.get("sheet_name") or "ОтчётРК"
            )
            headers = [
                "upd_num",
                "upd_time",
                "upd_sum",
                "advert_id",
                "camp_name",
                "advert_type",
                "payment_type",
                "advert_status",
                "sid",
            ]
            if ss_id and GoogleSheetsClient is not None:
                client = GoogleSheetsClient.from_env()
                if client is not None:
                    rows_2d = [[row.get(h, "") for h in headers] for row in data_items]
                    try:
                        write_res = client.replace_sheet_table(
                            str(ss_id), str(sheet_name), headers, rows_2d
                        )
                        return {
                            "success": True,
                            "result": f"written {write_res.get('written', 0)} rows",
                            "action": action,
                            "written": write_res.get("written", 0),
                        }
                    except Exception as exc:
                        return {
                            "success": False,
                            "message": f"failed to write sheet: {exc}",
                            "action": action,
                            "dataItems": data_items,
                        }

            return {
                "success": True,
                "result": f"fetched {len(data_items)} rows",
                "action": action,
                "headers": headers,
                "dataItems": data_items,
            }

        if action == "sheets/wb-plus/format-rules/reset":
            ss_id = payload.get("ssId") or payload.get("spreadsheet_id")
            sheet_name = payload.get("sheetName") or payload.get("sheet_name") or "WB+"
            if not ss_id:
                return {"success": False, "message": "ssId/spreadsheet_id is required"}
            if GoogleSheetsClient is None:
                return {
                    "success": False,
                    "message": "Google Sheets client is not available in this runtime",
                    "action": action,
                }
            client = GoogleSheetsClient.from_env()
            if client is None:
                return {
                    "success": False,
                    "message": (
                        "Google service account is not configured. "
                        "Set BTLZ_GOOGLE_SA_JSON or BTLZ_GOOGLE_SA_FILE and share the sheet with the SA email."
                    ),
                    "action": action,
                }
            try:
                res = client.dedupe_conditional_format_rules(
                    str(ss_id), str(sheet_name)
                )
            except Exception as exc:
                return {"success": False, "message": str(exc), "action": action}
            return {
                "success": True,
                "result": res.get("message", "ok"),
                "action": action,
                "data": res,
            }

        if action == "search/keys/refresh":
            ss_id = payload.get("ssId") or payload.get("spreadsheet_id")
            if not ss_id:
                return {"success": False, "message": "ssId/spreadsheet_id is required"}
            date_from = str(payload.get("dateFrom") or payload.get("date_from") or "")[
                :10
            ]
            date_to = str(payload.get("dateTo") or payload.get("date_to") or "")[:10]
            if not date_from:
                date_from = (datetime.utcnow() - timedelta(days=30)).strftime(
                    "%Y-%m-%d"
                )
            if not date_to:
                date_to = datetime.utcnow().strftime("%Y-%m-%d")
            nm_ids = payload.get("nm_ids") or payload.get("nmIds") or []
            if not isinstance(nm_ids, list):
                nm_ids = []
            nm_ids_int = []
            for item in nm_ids:
                try:
                    v = int(float(item))
                except (TypeError, ValueError):
                    continue
                if v:
                    nm_ids_int.append(v)

            # Build keys from adv normquery rows (top queries per nm_id).
            data_items: List[Dict[str, Any]] = []
            for token_item in self.storage.list_wb_tokens(str(ss_id)):
                if not token_item.is_active:
                    continue
                try:
                    rows = build_adv_normquery_rows(
                        token_item.token,
                        date_from,
                        date_to,
                        nm_ids=nm_ids_int,
                        min_views=10,
                    )
                except Exception:
                    rows = []
                items = build_search_items_from_adv_rows(
                    rows, max_keys_per_nm=5, min_views=10
                )
                for it in items:
                    nm_id = it.get("nm_id") or it.get("itemNumber") or it.get("nmId")
                    query = it.get("query") or it.get("search_key")
                    if not nm_id or not query:
                        continue
                    data_items.append(
                        {
                            "nm_id": int(nm_id),
                            "search_key": str(query),
                            "search_key_id": f"{int(nm_id)}__{str(query)}",
                        }
                    )

            stored_count = self.storage.upsert_search_keys(
                str(ss_id), data_items, source="adv_normquery"
            )

            sheet_name = (
                payload.get("sheetName") or payload.get("sheet_name") or "search_keys"
            )
            headers = ["nm_id", "search_key", "search_key_id"]
            if GoogleSheetsClient is not None:
                client = GoogleSheetsClient.from_env()
                if client is not None:
                    try:
                        rows_2d = [
                            [row.get(h, "") for h in headers] for row in data_items
                        ]
                        client.replace_sheet_table(
                            str(ss_id), str(sheet_name), headers, rows_2d
                        )
                        return {
                            "success": True,
                            "result": f"written {len(rows_2d)} rows",
                            "action": action,
                            "stored": stored_count,
                        }
                    except Exception as exc:
                        return {
                            "success": False,
                            "message": str(exc),
                            "action": action,
                            "dataItems": data_items,
                        }

            return {
                "success": True,
                "result": f"fetched {len(data_items)} rows",
                "action": action,
                "headers": headers,
                "dataItems": data_items,
                "stored": stored_count,
            }

        if action in {"collector/run", "wb/collector/run"}:
            ss_id = payload.get("ssId") or payload.get("spreadsheet_id")
            if not ss_id:
                return {"success": False, "message": "ssId/spreadsheet_id is required"}
            date_from = str(payload.get("dateFrom") or payload.get("date_from") or "")[
                :10
            ]
            date_to = str(payload.get("dateTo") or payload.get("date_to") or "")[:10]
            if not date_from:
                date_from = datetime.utcnow().strftime("%Y-%m-%d")
            if not date_to:
                date_to = date_from
            nm_ids_raw = payload.get("nm_ids") or payload.get("nmIds") or []
            nm_ids: List[int] = []
            if isinstance(nm_ids_raw, list):
                for item in nm_ids_raw:
                    try:
                        val = int(float(item))
                    except (TypeError, ValueError):
                        continue
                    if val:
                        nm_ids.append(val)
            with_positions = not bool(
                payload.get("noPositions") or payload.get("no_positions")
            )
            with_funnel = not bool(payload.get("noFunnel") or payload.get("no_funnel"))

            owner = "api:unknown"
            if auth is not None:
                actor = str(auth.actor or "unknown")
                role = str(auth.role or "unknown")
                owner = f"api:{role}:{actor}:{os.getpid()}"
            acquired, lock_state = self.storage.acquire_collector_lock(
                owner=owner,
                ttl_seconds=COLLECTOR_LOCK_TTL_SECONDS,
            )
            if not acquired:
                _metric_inc("http_collector_lock_conflicts_total")
                return {
                    "success": False,
                    "action": action,
                    "message": "collector is already running",
                    "lock": lock_state,
                }

            heartbeat_stop = threading.Event()
            heartbeat_lost: Dict[str, bool] = {"lost": False}

            def _heartbeat() -> None:
                interval = max(
                    15,
                    min(120, int(COLLECTOR_LOCK_TTL_SECONDS // 3) or 15),
                )
                while not heartbeat_stop.wait(interval):
                    ok = self.storage.refresh_collector_lock(
                        owner=owner,
                        ttl_seconds=COLLECTOR_LOCK_TTL_SECONDS,
                    )
                    if not ok:
                        heartbeat_lost["lost"] = True
                        break

            heartbeat_thread = threading.Thread(
                target=_heartbeat,
                name="collector-lock-heartbeat",
                daemon=True,
            )
            heartbeat_thread.start()

            try:
                summary = run_collection(
                    storage=self.storage,
                    spreadsheet_id=str(ss_id),
                    date_from=date_from,
                    date_to=date_to,
                    nm_ids=nm_ids if nm_ids else None,
                    with_positions=with_positions,
                    with_funnel=with_funnel,
                )
            except Exception as exc:
                return {"success": False, "message": str(exc), "action": action}
            finally:
                heartbeat_stop.set()
                heartbeat_thread.join(timeout=2)
                try:
                    self.storage.release_collector_lock(owner)
                except Exception:
                    pass
            if heartbeat_lost.get("lost"):
                _metric_inc("http_collector_lock_conflicts_total")
                return {
                    "success": False,
                    "action": action,
                    "message": "collector lock was lost during execution",
                }
            return {
                "success": True,
                "action": action,
                "result": "collector completed",
                "data": summary,
            }

        return {"success": True, "result": "accepted", "action": action}


def main() -> None:
    server = ThreadingHTTPServer((HOST, PORT), ApiHandler)
    print(f"[backend] listening on http://{HOST}:{PORT}")
    print(f"[backend] db path: {DB_PATH}")
    auth_modes = []
    if API_KEY:
        auth_modes.append("api_key")
    if BEARER_TOKENS:
        auth_modes.append("bearer")
    if auth_modes:
        print(f"[backend] auth protection: enabled ({', '.join(auth_modes)})")
    else:
        print("[backend] auth protection: NOT configured")
    server.serve_forever()


if __name__ == "__main__":
    main()
