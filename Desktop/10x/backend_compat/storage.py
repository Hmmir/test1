import json
import os
import sqlite3
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple


def _now_iso() -> str:
    return __import__("datetime").datetime.utcnow().isoformat(timespec="seconds") + "Z"


def _load_token_cipher() -> Any:
    key = str(os.environ.get("BTLZ_TOKEN_ENCRYPTION_KEY", "") or "").strip()
    if not key:
        return None
    try:
        from cryptography.fernet import Fernet
    except Exception as exc:
        raise RuntimeError(
            "BTLZ_TOKEN_ENCRYPTION_KEY is set, but 'cryptography' dependency is not installed"
        ) from exc
    try:
        return Fernet(key.encode("utf-8"))
    except Exception as exc:
        raise RuntimeError("BTLZ_TOKEN_ENCRYPTION_KEY is invalid for Fernet") from exc


TOKEN_ENC_PREFIX = "enc:v1:"


@dataclass
class WbTokenItem:
    token: str
    sid: Optional[str]
    is_active: bool


class Storage:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._lock = threading.Lock()
        self._token_cipher = _load_token_cipher()
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=10)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout = 10000")
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def _init_db(self) -> None:
        parent = os.path.dirname(self.db_path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        with self._connect() as conn:
            conn.execute("PRAGMA journal_mode = WAL")
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS spreadsheets (
                  spreadsheet_id TEXT PRIMARY KEY,
                  owner_email TEXT,
                  is_active INTEGER NOT NULL DEFAULT 1,
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS wb_tokens (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  spreadsheet_id TEXT NOT NULL,
                  token TEXT NOT NULL,
                  sid TEXT,
                  is_active INTEGER NOT NULL DEFAULT 1,
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL,
                  UNIQUE(spreadsheet_id, token)
                );

                CREATE TABLE IF NOT EXISTS audit_events (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  event_type TEXT NOT NULL,
                  actor TEXT,
                  role TEXT,
                  method TEXT,
                  path TEXT,
                  status_code INTEGER,
                  ip TEXT,
                  payload_json TEXT NOT NULL DEFAULT '{}',
                  created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS collector_run_locks (
                  lock_key TEXT PRIMARY KEY,
                  owner TEXT NOT NULL,
                  expires_at INTEGER NOT NULL,
                  updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS plan_month_save (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  spreadsheet_id TEXT NOT NULL,
                  nm_id INTEGER NOT NULL,
                  dt TEXT NOT NULL,
                  data_json TEXT NOT NULL,
                  created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS action_plan_tasks (
                  task_id INTEGER PRIMARY KEY AUTOINCREMENT,
                  spreadsheet_id TEXT NOT NULL,
                  nm_id INTEGER NOT NULL,
                  indicator TEXT,
                  problem TEXT,
                  hypothesis TEXT,
                  staff TEXT,
                  status INTEGER NOT NULL DEFAULT 0,
                  date_from TEXT,
                  date_to TEXT,
                  calendar_data TEXT NOT NULL DEFAULT '{}',
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS search_keys (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  spreadsheet_id TEXT NOT NULL,
                  nm_id INTEGER NOT NULL,
                  search_key TEXT NOT NULL,
                  search_key_id TEXT NOT NULL,
                  source TEXT NOT NULL DEFAULT 'manual',
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL,
                  UNIQUE(spreadsheet_id, search_key_id)
                );

                CREATE TABLE IF NOT EXISTS daily_positions (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  spreadsheet_id TEXT NOT NULL,
                  dt TEXT NOT NULL,
                  nm_id INTEGER NOT NULL,
                  search_key TEXT NOT NULL,
                  position INTEGER NOT NULL DEFAULT 0,
                  promo_position INTEGER NOT NULL DEFAULT 0,
                  source TEXT NOT NULL DEFAULT 'search_api',
                  raw_json TEXT NOT NULL DEFAULT '{}',
                  captured_at TEXT NOT NULL,
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL,
                  UNIQUE(spreadsheet_id, dt, nm_id, search_key)
                );

                -- Raw per-destination positions (multi-region). The original daily_positions table
                -- remains the aggregated view used by Apps Script sheets.
                CREATE TABLE IF NOT EXISTS daily_positions_geo (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  spreadsheet_id TEXT NOT NULL,
                  dt TEXT NOT NULL,
                  nm_id INTEGER NOT NULL,
                  search_key TEXT NOT NULL,
                  dest TEXT NOT NULL,
                  position INTEGER NOT NULL DEFAULT 0,
                  promo_position INTEGER NOT NULL DEFAULT 0,
                  source TEXT NOT NULL DEFAULT 'search_api',
                  raw_json TEXT NOT NULL DEFAULT '{}',
                  captured_at TEXT NOT NULL,
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL,
                  UNIQUE(spreadsheet_id, dt, nm_id, search_key, dest)
                );

                CREATE TABLE IF NOT EXISTS daily_adv (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  spreadsheet_id TEXT NOT NULL,
                  dt TEXT NOT NULL,
                  nm_id INTEGER NOT NULL,
                  adv_sum_total REAL NOT NULL DEFAULT 0,
                  adv_sum_auto REAL NOT NULL DEFAULT 0,
                  adv_sum_search REAL NOT NULL DEFAULT 0,
                  adv_sum_unknown REAL NOT NULL DEFAULT 0,
                  source TEXT NOT NULL DEFAULT 'adv_upd',
                  raw_json TEXT NOT NULL DEFAULT '{}',
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL,
                  UNIQUE(spreadsheet_id, dt, nm_id)
                );

                CREATE TABLE IF NOT EXISTS daily_stocks (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  spreadsheet_id TEXT NOT NULL,
                  dt TEXT NOT NULL,
                  nm_id INTEGER NOT NULL,
                  stocks_wb INTEGER NOT NULL DEFAULT 0,
                  in_way_to_client INTEGER NOT NULL DEFAULT 0,
                  in_way_from_client INTEGER NOT NULL DEFAULT 0,
                  stocks_mp INTEGER NOT NULL DEFAULT 0,
                  source TEXT NOT NULL DEFAULT 'wb_stats',
                  raw_json TEXT NOT NULL DEFAULT '{}',
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL,
                  UNIQUE(spreadsheet_id, dt, nm_id)
                );

                CREATE TABLE IF NOT EXISTS daily_prices (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  spreadsheet_id TEXT NOT NULL,
                  dt TEXT NOT NULL,
                  nm_id INTEGER NOT NULL,
                  discounted_price REAL NOT NULL DEFAULT 0,
                  discounted_price_with_spp REAL NOT NULL DEFAULT 0,
                  spp REAL NOT NULL DEFAULT 0,
                  source TEXT NOT NULL DEFAULT 'cards',
                  raw_json TEXT NOT NULL DEFAULT '{}',
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL,
                  UNIQUE(spreadsheet_id, dt, nm_id)
                );

                CREATE TABLE IF NOT EXISTS daily_unit_settings (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  spreadsheet_id TEXT NOT NULL,
                  dt TEXT NOT NULL,
                  nm_id INTEGER NOT NULL,
                  data_json TEXT NOT NULL,
                  source TEXT NOT NULL DEFAULT 'unit_sheet',
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL,
                  UNIQUE(spreadsheet_id, dt, nm_id)
                );

                CREATE TABLE IF NOT EXISTS wb_commission_rates (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  dt TEXT NOT NULL,
                  subject_id INTEGER NOT NULL,
                  subject_name TEXT,
                  parent_id INTEGER,
                  parent_name TEXT,
                  kgvp_booking REAL NOT NULL DEFAULT 0,
                  kgvp_marketplace REAL NOT NULL DEFAULT 0,
                  kgvp_pickup REAL NOT NULL DEFAULT 0,
                  kgvp_supplier REAL NOT NULL DEFAULT 0,
                  kgvp_supplier_express REAL NOT NULL DEFAULT 0,
                  paid_storage_kgvp REAL NOT NULL DEFAULT 0,
                  raw_json TEXT NOT NULL DEFAULT '{}',
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL,
                  UNIQUE(dt, subject_id)
                );

                CREATE TABLE IF NOT EXISTS wb_box_tariffs (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  dt TEXT NOT NULL,
                  warehouse_id INTEGER,
                  warehouse_name TEXT,
                  geo_name TEXT,
                  box_delivery_base REAL NOT NULL DEFAULT 0,
                  box_delivery_liter REAL NOT NULL DEFAULT 0,
                  box_storage_base REAL NOT NULL DEFAULT 0,
                  box_storage_liter REAL NOT NULL DEFAULT 0,
                  raw_json TEXT NOT NULL DEFAULT '{}',
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL,
                  UNIQUE(dt, warehouse_id, warehouse_name, geo_name)
                );

                CREATE TABLE IF NOT EXISTS daily_funnel (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  spreadsheet_id TEXT NOT NULL,
                  dt TEXT NOT NULL,
                  nm_id INTEGER NOT NULL,
                  open_card_count INTEGER NOT NULL DEFAULT 0,
                  add_to_cart_count INTEGER NOT NULL DEFAULT 0,
                  add_to_wishlist_count INTEGER NOT NULL DEFAULT 0,
                  source TEXT NOT NULL DEFAULT 'sales_funnel_history',
                  raw_json TEXT NOT NULL DEFAULT '{}',
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL,
                  UNIQUE(spreadsheet_id, dt, nm_id)
                );

                CREATE TABLE IF NOT EXISTS daily_detail_history (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  spreadsheet_id TEXT NOT NULL,
                  dt TEXT NOT NULL,
                  nm_id INTEGER NOT NULL,
                  open_card_count INTEGER NOT NULL DEFAULT 0,
                  add_to_cart_count INTEGER NOT NULL DEFAULT 0,
                  add_to_wishlist_count INTEGER NOT NULL DEFAULT 0,
                  orders_count INTEGER NOT NULL DEFAULT 0,
                  orders_sum_rub REAL NOT NULL DEFAULT 0,
                  buyouts_count INTEGER NOT NULL DEFAULT 0,
                  buyouts_sum_rub REAL NOT NULL DEFAULT 0,
                  cancel_count INTEGER NOT NULL DEFAULT 0,
                  cancel_sum_rub REAL NOT NULL DEFAULT 0,
                  add_to_cart_conversion REAL NOT NULL DEFAULT 0,
                  cart_to_order_conversion REAL NOT NULL DEFAULT 0,
                  buyout_percent REAL NOT NULL DEFAULT 0,
                  currency TEXT,
                  source TEXT NOT NULL DEFAULT 'detail_history_report',
                  raw_json TEXT NOT NULL DEFAULT '{}',
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL,
                  UNIQUE(spreadsheet_id, dt, nm_id)
                );

                CREATE TABLE IF NOT EXISTS daily_localization (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  spreadsheet_id TEXT NOT NULL,
                  dt TEXT NOT NULL,
                  nm_id INTEGER NOT NULL,
                  orders_count_total INTEGER NOT NULL DEFAULT 0,
                  orders_count_local INTEGER NOT NULL DEFAULT 0,
                  localization_percent REAL NOT NULL DEFAULT 0,
                  orders_count_total_central INTEGER NOT NULL DEFAULT 0,
                  orders_count_total_northwest INTEGER NOT NULL DEFAULT 0,
                  orders_count_total_south_caucasus INTEGER NOT NULL DEFAULT 0,
                  orders_count_total_volga INTEGER NOT NULL DEFAULT 0,
                  orders_count_total_fareast INTEGER NOT NULL DEFAULT 0,
                  orders_count_total_ural INTEGER NOT NULL DEFAULT 0,
                  orders_count_local_central INTEGER NOT NULL DEFAULT 0,
                  orders_count_local_northwest INTEGER NOT NULL DEFAULT 0,
                  orders_count_local_south_caucasus INTEGER NOT NULL DEFAULT 0,
                  orders_count_local_volga INTEGER NOT NULL DEFAULT 0,
                  orders_count_local_fareast INTEGER NOT NULL DEFAULT 0,
                  orders_count_local_ural INTEGER NOT NULL DEFAULT 0,
                  source TEXT NOT NULL DEFAULT 'orders_stats',
                  raw_json TEXT NOT NULL DEFAULT '{}',
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL,
                  UNIQUE(spreadsheet_id, dt, nm_id)
                );

                CREATE INDEX IF NOT EXISTS idx_daily_positions_main
                  ON daily_positions(spreadsheet_id, dt, nm_id);
                CREATE INDEX IF NOT EXISTS idx_daily_positions_geo_main
                  ON daily_positions_geo(spreadsheet_id, dt, nm_id, dest);
                CREATE INDEX IF NOT EXISTS idx_daily_adv_main
                  ON daily_adv(spreadsheet_id, dt, nm_id);
                CREATE INDEX IF NOT EXISTS idx_daily_stocks_main
                  ON daily_stocks(spreadsheet_id, dt, nm_id);
                CREATE INDEX IF NOT EXISTS idx_daily_prices_main
                  ON daily_prices(spreadsheet_id, dt, nm_id);
                CREATE INDEX IF NOT EXISTS idx_daily_unit_settings_main
                  ON daily_unit_settings(spreadsheet_id, dt, nm_id);
                CREATE INDEX IF NOT EXISTS idx_wb_commission_rates_main
                  ON wb_commission_rates(dt, subject_id);
                CREATE INDEX IF NOT EXISTS idx_wb_box_tariffs_main
                  ON wb_box_tariffs(dt);
                CREATE INDEX IF NOT EXISTS idx_daily_funnel_main
                  ON daily_funnel(spreadsheet_id, dt, nm_id);
                CREATE INDEX IF NOT EXISTS idx_daily_detail_history_main
                  ON daily_detail_history(spreadsheet_id, dt, nm_id);
                CREATE INDEX IF NOT EXISTS idx_daily_localization_main
                  ON daily_localization(spreadsheet_id, dt, nm_id);
                """
            )

    def register_spreadsheet(
        self, spreadsheet_id: str, owner_email: Optional[str] = None
    ) -> None:
        now = _now_iso()
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO spreadsheets(spreadsheet_id, owner_email, is_active, created_at, updated_at)
                VALUES(?, ?, 1, ?, ?)
                ON CONFLICT(spreadsheet_id) DO UPDATE SET
                    owner_email=excluded.owner_email,
                    is_active=1,
                    updated_at=excluded.updated_at
                """,
                (spreadsheet_id, owner_email, now, now),
            )

    def is_spreadsheet_active(self, spreadsheet_id: str) -> bool:
        value = str(spreadsheet_id or "").strip()
        if not value:
            return False
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT is_active
                FROM spreadsheets
                WHERE spreadsheet_id = ?
                """,
                (value,),
            ).fetchone()
        if row is None:
            return False
        return bool(int(row["is_active"] or 0))

    def acquire_collector_lock(
        self,
        owner: str,
        ttl_seconds: int = 1800,
    ) -> Tuple[bool, Dict[str, Any]]:
        lock_owner = str(owner or "").strip() or "unknown"
        ttl = max(int(ttl_seconds or 60), 60)
        now_epoch = int(time.time())
        expires_at = now_epoch + ttl
        now = _now_iso()

        with self._lock, self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                """
                SELECT owner, expires_at
                FROM collector_run_locks
                WHERE lock_key = 'collector'
                """
            ).fetchone()

            if row is None:
                conn.execute(
                    """
                    INSERT INTO collector_run_locks(lock_key, owner, expires_at, updated_at)
                    VALUES('collector', ?, ?, ?)
                    """,
                    (lock_owner, expires_at, now),
                )
                return True, {"owner": lock_owner, "expires_at": expires_at}

            current_owner = str(row["owner"] or "")
            current_expires = int(row["expires_at"] or 0)
            if current_expires > now_epoch and current_owner != lock_owner:
                return False, {
                    "owner": current_owner,
                    "expires_at": current_expires,
                }

            conn.execute(
                """
                UPDATE collector_run_locks
                SET owner = ?, expires_at = ?, updated_at = ?
                WHERE lock_key = 'collector'
                """,
                (lock_owner, expires_at, now),
            )
        return True, {"owner": lock_owner, "expires_at": expires_at}

    def refresh_collector_lock(
        self,
        owner: str,
        ttl_seconds: int = 1800,
    ) -> bool:
        lock_owner = str(owner or "").strip()
        if not lock_owner:
            return False
        ttl = max(int(ttl_seconds or 60), 60)
        expires_at = int(time.time()) + ttl
        now = _now_iso()
        with self._lock, self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            cur = conn.execute(
                """
                UPDATE collector_run_locks
                SET expires_at = ?, updated_at = ?
                WHERE lock_key = 'collector' AND owner = ?
                """,
                (expires_at, now, lock_owner),
            )
            return bool(int(cur.rowcount or 0) > 0)

    def release_collector_lock(self, owner: str) -> None:
        lock_owner = str(owner or "").strip()
        with self._lock, self._connect() as conn:
            if lock_owner:
                conn.execute(
                    """
                    DELETE FROM collector_run_locks
                    WHERE lock_key = 'collector' AND owner = ?
                    """,
                    (lock_owner,),
                )
            else:
                conn.execute(
                    "DELETE FROM collector_run_locks WHERE lock_key = 'collector'"
                )

    def _encrypt_token(self, token: str) -> str:
        value = str(token or "")
        if not self._token_cipher:
            return value
        encrypted = self._token_cipher.encrypt(value.encode("utf-8")).decode("utf-8")
        return TOKEN_ENC_PREFIX + encrypted

    def _decrypt_token(self, raw: str) -> str:
        value = str(raw or "")
        if not value.startswith(TOKEN_ENC_PREFIX):
            return value
        if not self._token_cipher:
            raise RuntimeError(
                "Encrypted WB token found in DB, but BTLZ_TOKEN_ENCRYPTION_KEY is not configured"
            )
        payload = value[len(TOKEN_ENC_PREFIX) :]
        try:
            return self._token_cipher.decrypt(payload.encode("utf-8")).decode("utf-8")
        except Exception as exc:
            raise RuntimeError("Failed to decrypt WB token from DB") from exc

    def _upsert_wb_token_row(
        self,
        conn: sqlite3.Connection,
        row_id: int,
        encrypted_token: str,
        sid: Optional[str],
        now: str,
    ) -> None:
        conn.execute(
            """
            UPDATE wb_tokens
            SET token = ?,
                sid = ?,
                is_active = 1,
                updated_at = ?
            WHERE id = ?
            """,
            (encrypted_token, sid, now, row_id),
        )

    def add_wb_token(self, spreadsheet_id: str, token: str, sid: Optional[str]) -> None:
        now = _now_iso()
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                """
                SELECT id, token
                FROM wb_tokens
                WHERE spreadsheet_id = ?
                ORDER BY id DESC
                """,
                (spreadsheet_id,),
            ).fetchall()
            for row in rows:
                existing_token = self._decrypt_token(str(row["token"] or ""))
                if existing_token == token:
                    encrypted = self._encrypt_token(token)
                    self._upsert_wb_token_row(
                        conn,
                        int(row["id"]),
                        encrypted,
                        sid,
                        now,
                    )
                    return

            encrypted_token = self._encrypt_token(token)
            conn.execute(
                """
                INSERT INTO wb_tokens(spreadsheet_id, token, sid, is_active, created_at, updated_at)
                VALUES(?, ?, ?, 1, ?, ?)
                ON CONFLICT(spreadsheet_id, token) DO UPDATE SET
                    sid=excluded.sid,
                    is_active=1,
                    updated_at=excluded.updated_at
                """,
                (spreadsheet_id, encrypted_token, sid, now, now),
            )

    def list_wb_tokens(self, spreadsheet_id: str) -> List[WbTokenItem]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT id, token, sid, is_active
                FROM wb_tokens
                WHERE spreadsheet_id = ?
                ORDER BY id DESC
                """,
                (spreadsheet_id,),
            ).fetchall()

        token_items: List[WbTokenItem] = []
        rows_to_reencrypt: List[Tuple[int, str, Optional[str]]] = []
        for row in rows:
            raw_token = str(row["token"] or "")
            plain_token = self._decrypt_token(raw_token)
            if (
                self._token_cipher
                and raw_token
                and not raw_token.startswith(TOKEN_ENC_PREFIX)
            ):
                rows_to_reencrypt.append(
                    (int(row["id"]), self._encrypt_token(plain_token), row["sid"])
                )
            token_items.append(
                WbTokenItem(
                    token=plain_token,
                    sid=row["sid"],
                    is_active=bool(row["is_active"]),
                )
            )

        if rows_to_reencrypt:
            now = _now_iso()
            with self._lock, self._connect() as conn:
                for row_id, encrypted, sid in rows_to_reencrypt:
                    self._upsert_wb_token_row(
                        conn,
                        row_id,
                        encrypted,
                        sid,
                        now,
                    )

        return token_items

    def save_plan_month_items(
        self, spreadsheet_id: str, data: List[Dict[str, Any]]
    ) -> int:
        now = _now_iso()
        inserted = 0
        with self._lock, self._connect() as conn:
            for item in data:
                nm_id = int(item.get("nm_id") or 0)
                dt = str(item.get("date") or "")
                if not nm_id or not dt:
                    continue
                conn.execute(
                    """
                    INSERT INTO plan_month_save(spreadsheet_id, nm_id, dt, data_json, created_at)
                    VALUES(?, ?, ?, ?, ?)
                    """,
                    (
                        spreadsheet_id,
                        nm_id,
                        dt,
                        json.dumps(item, ensure_ascii=False),
                        now,
                    ),
                )
                inserted += 1
        return inserted

    def get_latest_plan_month_items(
        self, spreadsheet_id: str, nm_ids: Optional[List[int]] = None
    ) -> Dict[int, Dict[str, Any]]:
        query = """
            SELECT nm_id, data_json
            FROM plan_month_save
            WHERE spreadsheet_id = ?
        """
        args: List[Any] = [spreadsheet_id]
        if nm_ids:
            marks = ",".join("?" for _ in nm_ids)
            query += f" AND nm_id IN ({marks})"
            args.extend(nm_ids)
        query += " ORDER BY nm_id ASC, dt DESC, id DESC"

        with self._connect() as conn:
            rows = conn.execute(query, args).fetchall()

        result: Dict[int, Dict[str, Any]] = {}
        for row in rows:
            nm_id = int(row["nm_id"])
            if nm_id in result:
                continue
            try:
                payload = json.loads(row["data_json"])
            except json.JSONDecodeError:
                payload = {}
            if not isinstance(payload, dict):
                payload = {}
            result[nm_id] = payload
        return result

    def get_action_plan(
        self, spreadsheet_id: str, nm_ids: List[int], months: List[str]
    ) -> List[Dict[str, Any]]:
        if not nm_ids:
            return []
        marks = ",".join("?" for _ in nm_ids)
        args: List[Any] = [spreadsheet_id, *nm_ids]
        where_month = []
        for month in months:
            where_month.append("date_from LIKE ?")
            where_month.append("date_to LIKE ?")
            args.append(f"{month}%")
            args.append(f"{month}%")

        month_sql = ""
        if where_month:
            month_sql = " AND (" + " OR ".join(where_month) + ")"

        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT task_id, nm_id, indicator, problem, hypothesis, staff, status, date_from, date_to, calendar_data
                FROM action_plan_tasks
                WHERE spreadsheet_id = ?
                  AND nm_id IN ({marks})
                  {month_sql}
                ORDER BY nm_id, task_id
                """,
                args,
            ).fetchall()

        out: List[Dict[str, Any]] = []
        for row in rows:
            cal = row["calendar_data"] or "{}"
            try:
                calendar_data = json.loads(cal)
            except json.JSONDecodeError:
                calendar_data = {}
            out.append(
                {
                    "task_id": row["task_id"],
                    "nm_id": row["nm_id"],
                    "indicator": row["indicator"] or "",
                    "problem": row["problem"] or "",
                    "hypothesis": row["hypothesis"] or "",
                    "staff": row["staff"],
                    "status": bool(row["status"]),
                    "date_from": row["date_from"],
                    "date_to": row["date_to"],
                    "calendar_data": calendar_data,
                }
            )
        return out

    def upsert_action_plan_items(
        self, spreadsheet_id: str, items: List[Dict[str, Any]]
    ) -> int:
        now = _now_iso()
        affected = 0
        with self._lock, self._connect() as conn:
            for item in items:
                nm_id = int(item.get("nm_id") or 0)
                if not nm_id:
                    continue

                calendar_data = item.get("calendar_data") or {}
                if not isinstance(calendar_data, dict):
                    calendar_data = {}

                task_id = item.get("task_id")
                common_values = {
                    "spreadsheet_id": spreadsheet_id,
                    "nm_id": nm_id,
                    "indicator": (item.get("indicator") or "").strip(),
                    "problem": (item.get("problem") or "").strip(),
                    "hypothesis": (item.get("hypothesis") or "").strip(),
                    "staff": (item.get("staff") or None),
                    "status": 1 if item.get("status") else 0,
                    "date_from": item.get("date_from"),
                    "date_to": item.get("date_to"),
                    "calendar_data": json.dumps(calendar_data, ensure_ascii=False),
                    "updated_at": now,
                }

                if task_id:
                    conn.execute(
                        """
                        UPDATE action_plan_tasks
                        SET nm_id=:nm_id,
                            indicator=:indicator,
                            problem=:problem,
                            hypothesis=:hypothesis,
                            staff=:staff,
                            status=:status,
                            date_from=:date_from,
                            date_to=:date_to,
                            calendar_data=:calendar_data,
                            updated_at=:updated_at
                        WHERE spreadsheet_id=:spreadsheet_id AND task_id=:task_id
                        """,
                        {**common_values, "task_id": int(task_id)},
                    )
                    if conn.total_changes > 0:
                        affected += 1
                    continue

                conn.execute(
                    """
                    INSERT INTO action_plan_tasks(
                        spreadsheet_id, nm_id, indicator, problem, hypothesis, staff, status,
                        date_from, date_to, calendar_data, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        common_values["spreadsheet_id"],
                        common_values["nm_id"],
                        common_values["indicator"],
                        common_values["problem"],
                        common_values["hypothesis"],
                        common_values["staff"],
                        common_values["status"],
                        common_values["date_from"],
                        common_values["date_to"],
                        common_values["calendar_data"],
                        now,
                        now,
                    ),
                )
                affected += 1
        return affected

    def delete_action_plan_items(
        self,
        spreadsheet_id: str,
        task_ids: List[int],
        logs_to_delete: List[Dict[str, Any]],
    ) -> int:
        affected = 0
        with self._lock, self._connect() as conn:
            if task_ids:
                marks = ",".join("?" for _ in task_ids)
                args: List[Any] = [spreadsheet_id, *task_ids]
                conn.execute(
                    f"""
                    DELETE FROM action_plan_tasks
                    WHERE spreadsheet_id = ? AND task_id IN ({marks})
                    """,
                    args,
                )
                affected += conn.total_changes

            for item in logs_to_delete:
                task_id = int(item.get("task_id") or 0)
                dt = str(item.get("date") or "")
                if not task_id or not dt:
                    continue

                row = conn.execute(
                    """
                    SELECT calendar_data
                    FROM action_plan_tasks
                    WHERE spreadsheet_id = ? AND task_id = ?
                    """,
                    (spreadsheet_id, task_id),
                ).fetchone()
                if not row:
                    continue

                try:
                    calendar_data = json.loads(row["calendar_data"] or "{}")
                except json.JSONDecodeError:
                    calendar_data = {}

                if dt in calendar_data:
                    del calendar_data[dt]
                    conn.execute(
                        """
                        UPDATE action_plan_tasks
                        SET calendar_data = ?, updated_at = ?
                        WHERE spreadsheet_id = ? AND task_id = ?
                        """,
                        (
                            json.dumps(calendar_data, ensure_ascii=False),
                            _now_iso(),
                            spreadsheet_id,
                            task_id,
                        ),
                    )
                    affected += 1
        return affected

    def upsert_search_keys(
        self, spreadsheet_id: str, items: List[Dict[str, Any]], source: str = "manual"
    ) -> int:
        now = _now_iso()
        affected = 0
        with self._lock, self._connect() as conn:
            for item in items:
                if not isinstance(item, dict):
                    continue
                nm_id = int(item.get("nm_id") or 0)
                search_key = str(item.get("search_key") or "").strip()
                search_key_id = str(item.get("search_key_id") or "").strip()
                if not nm_id or not search_key:
                    continue
                if not search_key_id:
                    search_key_id = f"{nm_id}__{search_key}"
                conn.execute(
                    """
                    INSERT INTO search_keys(
                        spreadsheet_id, nm_id, search_key, search_key_id, source, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(spreadsheet_id, search_key_id) DO UPDATE SET
                      nm_id=excluded.nm_id,
                      search_key=excluded.search_key,
                      source=excluded.source,
                      updated_at=excluded.updated_at
                    """,
                    (
                        spreadsheet_id,
                        nm_id,
                        search_key,
                        search_key_id,
                        str(item.get("source") or source),
                        now,
                        now,
                    ),
                )
                affected += 1
        return affected

    def list_search_keys(
        self, spreadsheet_id: str, nm_ids: Optional[List[int]] = None
    ) -> List[Dict[str, Any]]:
        sql = """
            SELECT nm_id, search_key, search_key_id, source, updated_at
            FROM search_keys
            WHERE spreadsheet_id = ?
        """
        args: List[Any] = [spreadsheet_id]
        if nm_ids:
            marks = ",".join("?" for _ in nm_ids)
            sql += f" AND nm_id IN ({marks})"
            args.extend([int(x) for x in nm_ids])
        sql += " ORDER BY nm_id ASC, search_key ASC"
        with self._connect() as conn:
            rows = conn.execute(sql, args).fetchall()
        return [
            {
                "nm_id": int(row["nm_id"]),
                "search_key": str(row["search_key"] or ""),
                "search_key_id": str(row["search_key_id"] or ""),
                "source": str(row["source"] or ""),
                "updated_at": str(row["updated_at"] or ""),
            }
            for row in rows
        ]

    def upsert_daily_positions(
        self,
        spreadsheet_id: str,
        dt: str,
        rows: List[Dict[str, Any]],
        source: str = "search_api",
    ) -> int:
        day = str(dt or "")[:10]
        if not day:
            return 0
        now = _now_iso()
        affected = 0
        with self._lock, self._connect() as conn:
            for row in rows:
                if not isinstance(row, dict):
                    continue
                nm_id = int(
                    row.get("nm_id") or row.get("itemNumber") or row.get("nmId") or 0
                )
                search_key = str(
                    row.get("search_key") or row.get("query") or ""
                ).strip()
                if not nm_id or not search_key:
                    continue
                position = int(float(row.get("position") or 0))
                promo_position = int(
                    float(row.get("promo_position") or row.get("promoPosition") or 0)
                )
                conn.execute(
                    """
                    INSERT INTO daily_positions(
                      spreadsheet_id, dt, nm_id, search_key, position, promo_position,
                      source, raw_json, captured_at, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(spreadsheet_id, dt, nm_id, search_key) DO UPDATE SET
                      position=excluded.position,
                      promo_position=excluded.promo_position,
                      source=excluded.source,
                      raw_json=excluded.raw_json,
                      captured_at=excluded.captured_at,
                      updated_at=excluded.updated_at
                    """,
                    (
                        spreadsheet_id,
                        day,
                        nm_id,
                        search_key,
                        position,
                        promo_position,
                        str(row.get("source") or source),
                        json.dumps(row, ensure_ascii=False),
                        str(row.get("captured_at") or now),
                        now,
                        now,
                    ),
                )
                affected += 1
        return affected

    def upsert_daily_positions_geo(
        self,
        spreadsheet_id: str,
        dt: str,
        rows: List[Dict[str, Any]],
        source: str = "search_api",
    ) -> int:
        day = str(dt or "")[:10]
        if not day:
            return 0
        now = _now_iso()
        affected = 0
        with self._lock, self._connect() as conn:
            for row in rows:
                if not isinstance(row, dict):
                    continue
                nm_id = int(
                    row.get("nm_id") or row.get("itemNumber") or row.get("nmId") or 0
                )
                search_key = str(
                    row.get("search_key") or row.get("query") or ""
                ).strip()
                dest = str(
                    row.get("dest") or row.get("destination") or row.get("region") or ""
                ).strip()
                if not nm_id or not search_key or not dest:
                    continue
                position = int(float(row.get("position") or 0))
                promo_position = int(
                    float(row.get("promo_position") or row.get("promoPosition") or 0)
                )
                conn.execute(
                    """
                    INSERT INTO daily_positions_geo(
                      spreadsheet_id, dt, nm_id, search_key, dest, position, promo_position,
                      source, raw_json, captured_at, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(spreadsheet_id, dt, nm_id, search_key, dest) DO UPDATE SET
                      position=excluded.position,
                      promo_position=excluded.promo_position,
                      source=excluded.source,
                      raw_json=excluded.raw_json,
                      captured_at=excluded.captured_at,
                      updated_at=excluded.updated_at
                    """,
                    (
                        spreadsheet_id,
                        day,
                        nm_id,
                        search_key,
                        dest,
                        position,
                        promo_position,
                        str(row.get("source") or source),
                        json.dumps(row, ensure_ascii=False),
                        str(row.get("captured_at") or now),
                        now,
                        now,
                    ),
                )
                affected += 1
        return affected

    def get_daily_positions(
        self,
        spreadsheet_id: str,
        date_from: str,
        date_to: str,
        nm_ids: Optional[List[int]] = None,
    ) -> List[Dict[str, Any]]:
        sql = """
            SELECT dt, nm_id, search_key, position, promo_position, source, captured_at
            FROM daily_positions
            WHERE spreadsheet_id = ?
              AND dt >= ?
              AND dt <= ?
        """
        args: List[Any] = [spreadsheet_id, str(date_from)[:10], str(date_to)[:10]]
        if nm_ids:
            marks = ",".join("?" for _ in nm_ids)
            sql += f" AND nm_id IN ({marks})"
            args.extend([int(x) for x in nm_ids])
        sql += " ORDER BY dt ASC, nm_id ASC, search_key ASC"
        with self._connect() as conn:
            rows = conn.execute(sql, args).fetchall()
        return [
            {
                "date": str(row["dt"] or ""),
                "nm_id": int(row["nm_id"]),
                "search_key": str(row["search_key"] or ""),
                "position": int(row["position"] or 0),
                "promo_position": int(row["promo_position"] or 0),
                "source": str(row["source"] or ""),
                "captured_at": str(row["captured_at"] or ""),
            }
            for row in rows
        ]

    def upsert_daily_adv(
        self, spreadsheet_id: str, rows: List[Dict[str, Any]], source: str = "adv_upd"
    ) -> int:
        now = _now_iso()
        affected = 0
        with self._lock, self._connect() as conn:
            for row in rows:
                if not isinstance(row, dict):
                    continue
                day = str(row.get("date") or row.get("dt") or "")[:10]
                nm_id = int(row.get("nm_id") or row.get("nmId") or 0)
                if not day or not nm_id:
                    continue
                adv_sum_total = float(
                    row.get("adv_sum_total") or row.get("adv_sum") or 0.0
                )
                adv_sum_auto = float(row.get("adv_sum_auto") or 0.0)
                adv_sum_search = float(row.get("adv_sum_search") or 0.0)
                adv_sum_unknown = float(row.get("adv_sum_unknown") or 0.0)
                conn.execute(
                    """
                    INSERT INTO daily_adv(
                      spreadsheet_id, dt, nm_id, adv_sum_total, adv_sum_auto, adv_sum_search, adv_sum_unknown,
                      source, raw_json, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(spreadsheet_id, dt, nm_id) DO UPDATE SET
                      adv_sum_total=excluded.adv_sum_total,
                      adv_sum_auto=excluded.adv_sum_auto,
                      adv_sum_search=excluded.adv_sum_search,
                      adv_sum_unknown=excluded.adv_sum_unknown,
                      source=excluded.source,
                      raw_json=excluded.raw_json,
                      updated_at=excluded.updated_at
                    """,
                    (
                        spreadsheet_id,
                        day,
                        nm_id,
                        adv_sum_total,
                        adv_sum_auto,
                        adv_sum_search,
                        adv_sum_unknown,
                        str(row.get("source") or source),
                        json.dumps(row, ensure_ascii=False),
                        now,
                        now,
                    ),
                )
                affected += 1
        return affected

    def get_daily_adv(
        self,
        spreadsheet_id: str,
        date_from: str,
        date_to: str,
        nm_ids: Optional[List[int]] = None,
    ) -> List[Dict[str, Any]]:
        sql = """
            SELECT dt, nm_id, adv_sum_total, adv_sum_auto, adv_sum_search, adv_sum_unknown, source
            FROM daily_adv
            WHERE spreadsheet_id = ?
              AND dt >= ?
              AND dt <= ?
        """
        args: List[Any] = [spreadsheet_id, str(date_from)[:10], str(date_to)[:10]]
        if nm_ids:
            marks = ",".join("?" for _ in nm_ids)
            sql += f" AND nm_id IN ({marks})"
            args.extend([int(x) for x in nm_ids])
        sql += " ORDER BY dt ASC, nm_id ASC"
        with self._connect() as conn:
            rows = conn.execute(sql, args).fetchall()
        return [
            {
                "date": str(row["dt"] or ""),
                "nm_id": int(row["nm_id"]),
                "adv_sum_total": float(row["adv_sum_total"] or 0.0),
                "adv_sum_auto": float(row["adv_sum_auto"] or 0.0),
                "adv_sum_search": float(row["adv_sum_search"] or 0.0),
                "adv_sum_unknown": float(row["adv_sum_unknown"] or 0.0),
                "source": str(row["source"] or ""),
            }
            for row in rows
        ]

    def upsert_daily_stocks(
        self, spreadsheet_id: str, rows: List[Dict[str, Any]], source: str = "wb_stats"
    ) -> int:
        now = _now_iso()
        affected = 0
        with self._lock, self._connect() as conn:
            for row in rows:
                if not isinstance(row, dict):
                    continue
                day = str(row.get("date") or row.get("dt") or "")[:10]
                nm_id = int(row.get("nm_id") or row.get("nmId") or 0)
                if not day or not nm_id:
                    continue
                stocks_wb = int(float(row.get("stocks_wb") or row.get("stocks") or 0))
                in_way_to_client = int(
                    float(row.get("in_way_to_client") or row.get("inWayToClient") or 0)
                )
                in_way_from_client = int(
                    float(
                        row.get("in_way_from_client") or row.get("inWayFromClient") or 0
                    )
                )
                stocks_mp = int(
                    float(
                        row.get("stocks_mp") or (in_way_to_client + in_way_from_client)
                    )
                )
                conn.execute(
                    """
                    INSERT INTO daily_stocks(
                      spreadsheet_id, dt, nm_id, stocks_wb, in_way_to_client, in_way_from_client, stocks_mp,
                      source, raw_json, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(spreadsheet_id, dt, nm_id) DO UPDATE SET
                      stocks_wb=excluded.stocks_wb,
                      in_way_to_client=excluded.in_way_to_client,
                      in_way_from_client=excluded.in_way_from_client,
                      stocks_mp=excluded.stocks_mp,
                      source=excluded.source,
                      raw_json=excluded.raw_json,
                      updated_at=excluded.updated_at
                    """,
                    (
                        spreadsheet_id,
                        day,
                        nm_id,
                        stocks_wb,
                        in_way_to_client,
                        in_way_from_client,
                        stocks_mp,
                        str(row.get("source") or source),
                        json.dumps(row, ensure_ascii=False),
                        now,
                        now,
                    ),
                )
                affected += 1
        return affected

    def get_daily_stocks(
        self,
        spreadsheet_id: str,
        date_from: str,
        date_to: str,
        nm_ids: Optional[List[int]] = None,
    ) -> List[Dict[str, Any]]:
        sql = """
            SELECT dt, nm_id, stocks_wb, in_way_to_client, in_way_from_client, stocks_mp, source
            FROM daily_stocks
            WHERE spreadsheet_id = ?
              AND dt >= ?
              AND dt <= ?
        """
        args: List[Any] = [spreadsheet_id, str(date_from)[:10], str(date_to)[:10]]
        if nm_ids:
            marks = ",".join("?" for _ in nm_ids)
            sql += f" AND nm_id IN ({marks})"
            args.extend([int(x) for x in nm_ids])
        sql += " ORDER BY dt ASC, nm_id ASC"
        with self._connect() as conn:
            rows = conn.execute(sql, args).fetchall()
        return [
            {
                "date": str(row["dt"] or ""),
                "nm_id": int(row["nm_id"]),
                "stocks_wb": int(row["stocks_wb"] or 0),
                "in_way_to_client": int(row["in_way_to_client"] or 0),
                "in_way_from_client": int(row["in_way_from_client"] or 0),
                "stocks_mp": int(row["stocks_mp"] or 0),
                "source": str(row["source"] or ""),
            }
            for row in rows
        ]

    def upsert_daily_prices(
        self, spreadsheet_id: str, rows: List[Dict[str, Any]], source: str = "cards"
    ) -> int:
        now = _now_iso()
        affected = 0
        with self._lock, self._connect() as conn:
            for row in rows:
                if not isinstance(row, dict):
                    continue
                day = str(row.get("date") or row.get("dt") or "")[:10]
                nm_id = int(row.get("nm_id") or row.get("nmId") or 0)
                if not day or not nm_id:
                    continue
                discounted_price = float(row.get("discounted_price") or 0.0)
                discounted_price_with_spp = float(
                    row.get("discounted_price_with_spp")
                    or row.get("avg_price_with_spp")
                    or 0.0
                )
                spp = float(row.get("spp") or 0.0)
                conn.execute(
                    """
                    INSERT INTO daily_prices(
                      spreadsheet_id, dt, nm_id, discounted_price, discounted_price_with_spp, spp,
                      source, raw_json, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(spreadsheet_id, dt, nm_id) DO UPDATE SET
                      discounted_price=excluded.discounted_price,
                      discounted_price_with_spp=excluded.discounted_price_with_spp,
                      spp=excluded.spp,
                      source=excluded.source,
                      raw_json=excluded.raw_json,
                      updated_at=excluded.updated_at
                    """,
                    (
                        spreadsheet_id,
                        day,
                        nm_id,
                        discounted_price,
                        discounted_price_with_spp,
                        spp,
                        str(row.get("source") or source),
                        json.dumps(row, ensure_ascii=False),
                        now,
                        now,
                    ),
                )
                affected += 1
        return affected

    def get_daily_prices(
        self,
        spreadsheet_id: str,
        date_from: str,
        date_to: str,
        nm_ids: Optional[List[int]] = None,
    ) -> List[Dict[str, Any]]:
        sql = """
            SELECT dt, nm_id, discounted_price, discounted_price_with_spp, spp, source
            FROM daily_prices
            WHERE spreadsheet_id = ?
              AND dt >= ?
              AND dt <= ?
        """
        args: List[Any] = [spreadsheet_id, str(date_from)[:10], str(date_to)[:10]]
        if nm_ids:
            marks = ",".join("?" for _ in nm_ids)
            sql += f" AND nm_id IN ({marks})"
            args.extend([int(x) for x in nm_ids])
        sql += " ORDER BY dt ASC, nm_id ASC"
        with self._connect() as conn:
            rows = conn.execute(sql, args).fetchall()
        return [
            {
                "date": str(row["dt"] or ""),
                "nm_id": int(row["nm_id"]),
                "discounted_price": float(row["discounted_price"] or 0.0),
                "discounted_price_with_spp": float(
                    row["discounted_price_with_spp"] or 0.0
                ),
                "spp": float(row["spp"] or 0.0),
                "source": str(row["source"] or ""),
            }
            for row in rows
        ]

    def upsert_daily_unit_settings(
        self,
        spreadsheet_id: str,
        rows: List[Dict[str, Any]],
        source: str = "unit_sheet",
    ) -> int:
        now = _now_iso()
        affected = 0
        with self._lock, self._connect() as conn:
            for row in rows:
                if not isinstance(row, dict):
                    continue
                day = str(row.get("date") or row.get("dt") or "")[:10]
                nm_id = int(row.get("nm_id") or row.get("nmId") or 0)
                if not day or not nm_id:
                    continue
                conn.execute(
                    """
                    INSERT INTO daily_unit_settings(
                      spreadsheet_id, dt, nm_id, data_json, source, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(spreadsheet_id, dt, nm_id) DO UPDATE SET
                      data_json=excluded.data_json,
                      source=excluded.source,
                      updated_at=excluded.updated_at
                    """,
                    (
                        spreadsheet_id,
                        day,
                        nm_id,
                        json.dumps(row, ensure_ascii=False),
                        str(row.get("source") or source),
                        now,
                        now,
                    ),
                )
                affected += 1
        return affected

    def get_daily_unit_settings(
        self,
        spreadsheet_id: str,
        date_from: str,
        date_to: str,
        nm_ids: Optional[List[int]] = None,
    ) -> List[Dict[str, Any]]:
        sql = """
            SELECT dt, nm_id, data_json, source
            FROM daily_unit_settings
            WHERE spreadsheet_id = ?
              AND dt >= ?
              AND dt <= ?
        """
        args: List[Any] = [spreadsheet_id, str(date_from)[:10], str(date_to)[:10]]
        if nm_ids:
            marks = ",".join("?" for _ in nm_ids)
            sql += f" AND nm_id IN ({marks})"
            args.extend([int(x) for x in nm_ids])
        sql += " ORDER BY dt ASC, nm_id ASC"
        with self._connect() as conn:
            rows = conn.execute(sql, args).fetchall()

        out: List[Dict[str, Any]] = []
        for row in rows:
            payload = {}
            try:
                payload = json.loads(row["data_json"] or "{}")
            except json.JSONDecodeError:
                payload = {}
            if not isinstance(payload, dict):
                payload = {}
            payload["date"] = str(row["dt"] or "")
            payload["nm_id"] = int(row["nm_id"])
            payload["source"] = str(row["source"] or "")
            out.append(payload)
        return out

    def upsert_wb_commission_rates(self, dt: str, rows: List[Dict[str, Any]]) -> int:
        day = str(dt or "")[:10]
        if not day:
            return 0
        now = _now_iso()
        affected = 0
        with self._lock, self._connect() as conn:
            for row in rows:
                if not isinstance(row, dict):
                    continue
                try:
                    subject_id = int(
                        float(row.get("subjectID") or row.get("subject_id") or 0)
                    )
                except (TypeError, ValueError):
                    continue
                if not subject_id:
                    continue
                subject_name = str(
                    row.get("subjectName") or row.get("subject_name") or ""
                )
                parent_id = None
                try:
                    parent_id_raw = row.get("parentID") or row.get("parent_id")
                    if parent_id_raw is not None and parent_id_raw != "":
                        parent_id = int(float(parent_id_raw))
                except (TypeError, ValueError):
                    parent_id = None
                parent_name = str(row.get("parentName") or row.get("parent_name") or "")

                def _pct(key: str) -> float:
                    try:
                        return float(
                            str(row.get(key) or 0).replace(",", ".").replace(" ", "")
                        )
                    except Exception:
                        return 0.0

                conn.execute(
                    """
                    INSERT INTO wb_commission_rates(
                      dt, subject_id, subject_name, parent_id, parent_name,
                      kgvp_booking, kgvp_marketplace, kgvp_pickup, kgvp_supplier, kgvp_supplier_express,
                      paid_storage_kgvp, raw_json, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(dt, subject_id) DO UPDATE SET
                      subject_name=excluded.subject_name,
                      parent_id=excluded.parent_id,
                      parent_name=excluded.parent_name,
                      kgvp_booking=excluded.kgvp_booking,
                      kgvp_marketplace=excluded.kgvp_marketplace,
                      kgvp_pickup=excluded.kgvp_pickup,
                      kgvp_supplier=excluded.kgvp_supplier,
                      kgvp_supplier_express=excluded.kgvp_supplier_express,
                      paid_storage_kgvp=excluded.paid_storage_kgvp,
                      raw_json=excluded.raw_json,
                      updated_at=excluded.updated_at
                    """,
                    (
                        day,
                        subject_id,
                        subject_name,
                        parent_id,
                        parent_name,
                        _pct("kgvpBooking"),
                        _pct("kgvpMarketplace"),
                        _pct("kgvpPickup"),
                        _pct("kgvpSupplier"),
                        _pct("kgvpSupplierExpress"),
                        _pct("paidStorageKgvp"),
                        json.dumps(row, ensure_ascii=False),
                        now,
                        now,
                    ),
                )
                affected += 1
        return affected

    def get_latest_wb_commission_rates(
        self, date_to: str
    ) -> Tuple[str, Dict[int, Dict[str, Any]]]:
        """Return the latest available commission snapshot <= date_to.

        Returns (dt, by_subject_id). If not found, dt="" and map is empty.
        """

        day_to = str(date_to or "")[:10]
        if not day_to:
            return "", {}
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT dt
                FROM wb_commission_rates
                WHERE dt <= ?
                ORDER BY dt DESC
                LIMIT 1
                """,
                (day_to,),
            ).fetchone()
            if not row:
                return "", {}
            best_dt = str(row["dt"] or "")
            rows = conn.execute(
                """
                SELECT dt, subject_id, subject_name, parent_id, parent_name,
                       kgvp_booking, kgvp_marketplace, kgvp_pickup, kgvp_supplier, kgvp_supplier_express,
                       paid_storage_kgvp
                FROM wb_commission_rates
                WHERE dt = ?
                """,
                (best_dt,),
            ).fetchall()
        out: Dict[int, Dict[str, Any]] = {}
        for r in rows:
            sid = int(r["subject_id"] or 0)
            if not sid:
                continue
            out[sid] = {
                "date": str(r["dt"] or ""),
                "subject_id": sid,
                "subject_name": str(r["subject_name"] or ""),
                "parent_id": int(r["parent_id"])
                if r["parent_id"] is not None
                else None,
                "parent_name": str(r["parent_name"] or ""),
                "kgvp_booking": float(r["kgvp_booking"] or 0.0),
                "kgvp_marketplace": float(r["kgvp_marketplace"] or 0.0),
                "kgvp_pickup": float(r["kgvp_pickup"] or 0.0),
                "kgvp_supplier": float(r["kgvp_supplier"] or 0.0),
                "kgvp_supplier_express": float(r["kgvp_supplier_express"] or 0.0),
                "paid_storage_kgvp": float(r["paid_storage_kgvp"] or 0.0),
            }
        return best_dt, out

    def upsert_wb_box_tariffs(self, dt: str, rows: List[Dict[str, Any]]) -> int:
        day = str(dt or "")[:10]
        if not day:
            return 0
        now = _now_iso()
        affected = 0
        with self._lock, self._connect() as conn:
            for row in rows:
                if not isinstance(row, dict):
                    continue

                def _to_float_ru(value: Any) -> float:
                    if value is None:
                        return 0.0
                    if isinstance(value, (int, float)):
                        return float(value)
                    text = str(value).strip().replace(" ", "").replace(",", ".")
                    if not text:
                        return 0.0
                    try:
                        return float(text)
                    except ValueError:
                        return 0.0

                warehouse_id = None
                try:
                    w_raw = row.get("warehouseID") or row.get("warehouse_id")
                    if w_raw is not None and w_raw != "":
                        warehouse_id = int(float(w_raw))
                except (TypeError, ValueError):
                    warehouse_id = None
                warehouse_name = str(
                    row.get("warehouseName") or row.get("warehouse_name") or ""
                )
                geo_name = str(row.get("geoName") or row.get("geo_name") or "")
                conn.execute(
                    """
                    INSERT INTO wb_box_tariffs(
                      dt, warehouse_id, warehouse_name, geo_name,
                      box_delivery_base, box_delivery_liter, box_storage_base, box_storage_liter,
                      raw_json, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(dt, warehouse_id, warehouse_name, geo_name) DO UPDATE SET
                      box_delivery_base=excluded.box_delivery_base,
                      box_delivery_liter=excluded.box_delivery_liter,
                      box_storage_base=excluded.box_storage_base,
                      box_storage_liter=excluded.box_storage_liter,
                      raw_json=excluded.raw_json,
                      updated_at=excluded.updated_at
                    """,
                    (
                        day,
                        warehouse_id,
                        warehouse_name,
                        geo_name,
                        _to_float_ru(row.get("boxDeliveryBase")),
                        _to_float_ru(row.get("boxDeliveryLiter")),
                        _to_float_ru(row.get("boxStorageBase")),
                        _to_float_ru(row.get("boxStorageLiter")),
                        json.dumps(row, ensure_ascii=False),
                        now,
                        now,
                    ),
                )
                affected += 1
        return affected

    def upsert_daily_funnel(
        self,
        spreadsheet_id: str,
        rows: List[Dict[str, Any]],
        source: str = "sales_funnel_history",
    ) -> int:
        now = _now_iso()
        affected = 0
        with self._lock, self._connect() as conn:
            for row in rows:
                if not isinstance(row, dict):
                    continue
                day = str(row.get("date") or row.get("dt") or "")[:10]
                nm_id = int(row.get("nm_id") or row.get("nmId") or 0)
                if not day or not nm_id:
                    continue
                open_card_count = int(
                    float(row.get("open_card_count") or row.get("openCount") or 0)
                )
                add_to_cart_count = int(
                    float(row.get("add_to_cart_count") or row.get("cartCount") or 0)
                )
                add_to_wishlist_count = int(
                    float(
                        row.get("add_to_wishlist_count")
                        or row.get("addToWishlistCount")
                        or 0
                    )
                )
                conn.execute(
                    """
                    INSERT INTO daily_funnel(
                      spreadsheet_id, dt, nm_id, open_card_count, add_to_cart_count, add_to_wishlist_count,
                      source, raw_json, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(spreadsheet_id, dt, nm_id) DO UPDATE SET
                      open_card_count=excluded.open_card_count,
                      add_to_cart_count=excluded.add_to_cart_count,
                      add_to_wishlist_count=excluded.add_to_wishlist_count,
                      source=excluded.source,
                      raw_json=excluded.raw_json,
                      updated_at=excluded.updated_at
                    """,
                    (
                        spreadsheet_id,
                        day,
                        nm_id,
                        open_card_count,
                        add_to_cart_count,
                        add_to_wishlist_count,
                        str(row.get("source") or source),
                        json.dumps(row, ensure_ascii=False),
                        now,
                        now,
                    ),
                )
                affected += 1
        return affected

    def get_daily_funnel(
        self,
        spreadsheet_id: str,
        date_from: str,
        date_to: str,
        nm_ids: Optional[List[int]] = None,
    ) -> List[Dict[str, Any]]:
        sql = """
            SELECT dt, nm_id, open_card_count, add_to_cart_count, add_to_wishlist_count, source
            FROM daily_funnel
            WHERE spreadsheet_id = ?
              AND dt >= ?
              AND dt <= ?
        """
        args: List[Any] = [spreadsheet_id, str(date_from)[:10], str(date_to)[:10]]
        if nm_ids:
            marks = ",".join("?" for _ in nm_ids)
            sql += f" AND nm_id IN ({marks})"
            args.extend([int(x) for x in nm_ids])
        sql += " ORDER BY dt ASC, nm_id ASC"
        with self._connect() as conn:
            rows = conn.execute(sql, args).fetchall()
        return [
            {
                "date": str(row["dt"] or ""),
                "nm_id": int(row["nm_id"]),
                "open_card_count": int(row["open_card_count"] or 0),
                "add_to_cart_count": int(row["add_to_cart_count"] or 0),
                "add_to_wishlist_count": int(row["add_to_wishlist_count"] or 0),
                "source": str(row["source"] or ""),
            }
            for row in rows
        ]

    def upsert_daily_detail_history(
        self,
        spreadsheet_id: str,
        rows: List[Dict[str, Any]],
        source: str = "detail_history_report",
    ) -> int:
        now = _now_iso()
        affected = 0
        with self._lock, self._connect() as conn:
            for row in rows:
                if not isinstance(row, dict):
                    continue
                day = str(row.get("date") or row.get("dt") or "")[:10]
                nm_id = int(row.get("nm_id") or row.get("nmId") or row.get("nmID") or 0)
                if not day or not nm_id:
                    continue
                open_card_count = int(
                    float(row.get("open_card_count") or row.get("openCardCount") or 0)
                )
                add_to_cart_count = int(
                    float(
                        row.get("add_to_cart_count") or row.get("addToCartCount") or 0
                    )
                )
                add_to_wishlist_count = int(
                    float(
                        row.get("add_to_wishlist_count")
                        or row.get("addToWishlist")
                        or 0
                    )
                )
                orders_count = int(
                    float(row.get("orders_count") or row.get("ordersCount") or 0)
                )
                orders_sum_rub = float(
                    row.get("orders_sum_rub") or row.get("ordersSumRub") or 0.0
                )
                buyouts_count = int(
                    float(row.get("buyouts_count") or row.get("buyoutsCount") or 0)
                )
                buyouts_sum_rub = float(
                    row.get("buyouts_sum_rub") or row.get("buyoutsSumRub") or 0.0
                )
                cancel_count = int(
                    float(row.get("cancel_count") or row.get("cancelCount") or 0)
                )
                cancel_sum_rub = float(
                    row.get("cancel_sum_rub") or row.get("cancelSumRub") or 0.0
                )
                add_to_cart_conversion = float(
                    row.get("add_to_cart_conversion")
                    or row.get("addToCartConversion")
                    or 0.0
                )
                cart_to_order_conversion = float(
                    row.get("cart_to_order_conversion")
                    or row.get("cartToOrderConversion")
                    or 0.0
                )
                buyout_percent = float(
                    row.get("buyout_percent") or row.get("buyoutPercent") or 0.0
                )
                currency = str(row.get("currency") or "").strip() or None
                conn.execute(
                    """
                    INSERT INTO daily_detail_history(
                      spreadsheet_id, dt, nm_id,
                      open_card_count, add_to_cart_count, add_to_wishlist_count,
                      orders_count, orders_sum_rub, buyouts_count, buyouts_sum_rub,
                      cancel_count, cancel_sum_rub,
                      add_to_cart_conversion, cart_to_order_conversion, buyout_percent,
                      currency, source, raw_json, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(spreadsheet_id, dt, nm_id) DO UPDATE SET
                      open_card_count=excluded.open_card_count,
                      add_to_cart_count=excluded.add_to_cart_count,
                      add_to_wishlist_count=excluded.add_to_wishlist_count,
                      orders_count=excluded.orders_count,
                      orders_sum_rub=excluded.orders_sum_rub,
                      buyouts_count=excluded.buyouts_count,
                      buyouts_sum_rub=excluded.buyouts_sum_rub,
                      cancel_count=excluded.cancel_count,
                      cancel_sum_rub=excluded.cancel_sum_rub,
                      add_to_cart_conversion=excluded.add_to_cart_conversion,
                      cart_to_order_conversion=excluded.cart_to_order_conversion,
                      buyout_percent=excluded.buyout_percent,
                      currency=excluded.currency,
                      source=excluded.source,
                      raw_json=excluded.raw_json,
                      updated_at=excluded.updated_at
                    """,
                    (
                        spreadsheet_id,
                        day,
                        nm_id,
                        open_card_count,
                        add_to_cart_count,
                        add_to_wishlist_count,
                        orders_count,
                        orders_sum_rub,
                        buyouts_count,
                        buyouts_sum_rub,
                        cancel_count,
                        cancel_sum_rub,
                        add_to_cart_conversion,
                        cart_to_order_conversion,
                        buyout_percent,
                        currency,
                        str(row.get("source") or source),
                        json.dumps(row, ensure_ascii=False),
                        now,
                        now,
                    ),
                )
                affected += 1
        return affected

    def get_daily_detail_history(
        self,
        spreadsheet_id: str,
        date_from: str,
        date_to: str,
        nm_ids: Optional[List[int]] = None,
    ) -> List[Dict[str, Any]]:
        sql = """
            SELECT
              dt, nm_id,
              open_card_count, add_to_cart_count, add_to_wishlist_count,
              orders_count, orders_sum_rub, buyouts_count, buyouts_sum_rub,
              cancel_count, cancel_sum_rub,
              add_to_cart_conversion, cart_to_order_conversion, buyout_percent,
              currency, source
            FROM daily_detail_history
            WHERE spreadsheet_id = ?
              AND dt >= ?
              AND dt <= ?
        """
        args: List[Any] = [spreadsheet_id, str(date_from)[:10], str(date_to)[:10]]
        if nm_ids:
            marks = ",".join("?" for _ in nm_ids)
            sql += f" AND nm_id IN ({marks})"
            args.extend([int(x) for x in nm_ids])
        sql += " ORDER BY dt ASC, nm_id ASC"
        with self._connect() as conn:
            rows = conn.execute(sql, args).fetchall()
        return [
            {
                "date": str(row["dt"] or ""),
                "nm_id": int(row["nm_id"]),
                "open_card_count": int(row["open_card_count"] or 0),
                "add_to_cart_count": int(row["add_to_cart_count"] or 0),
                "add_to_wishlist_count": int(row["add_to_wishlist_count"] or 0),
                "orders_count": int(row["orders_count"] or 0),
                "orders_sum_rub": float(row["orders_sum_rub"] or 0.0),
                "buyouts_count": int(row["buyouts_count"] or 0),
                "buyouts_sum_rub": float(row["buyouts_sum_rub"] or 0.0),
                "cancel_count": int(row["cancel_count"] or 0),
                "cancel_sum_rub": float(row["cancel_sum_rub"] or 0.0),
                "add_to_cart_conversion": float(row["add_to_cart_conversion"] or 0.0),
                "cart_to_order_conversion": float(
                    row["cart_to_order_conversion"] or 0.0
                ),
                "buyout_percent": float(row["buyout_percent"] or 0.0),
                "currency": str(row["currency"] or ""),
                "source": str(row["source"] or ""),
            }
            for row in rows
        ]

    def upsert_daily_localization(
        self,
        spreadsheet_id: str,
        rows: List[Dict[str, Any]],
        source: str = "orders_stats",
    ) -> int:
        now = _now_iso()
        affected = 0
        with self._lock, self._connect() as conn:
            for row in rows:
                if not isinstance(row, dict):
                    continue
                day = str(row.get("date") or row.get("dt") or "")[:10]
                nm_id = int(row.get("nm_id") or row.get("nmId") or 0)
                if not day or not nm_id:
                    continue

                orders_count_total = int(float(row.get("orders_count_total") or 0))
                orders_count_local = int(float(row.get("orders_count_local") or 0))
                localization_percent = float(row.get("localization_percent") or 0.0)

                values = {
                    "orders_count_total_central": int(
                        float(row.get("orders_count_total_central") or 0)
                    ),
                    "orders_count_total_northwest": int(
                        float(row.get("orders_count_total_northwest") or 0)
                    ),
                    "orders_count_total_south_caucasus": int(
                        float(row.get("orders_count_total_south_caucasus") or 0)
                    ),
                    "orders_count_total_volga": int(
                        float(row.get("orders_count_total_volga") or 0)
                    ),
                    "orders_count_total_fareast": int(
                        float(row.get("orders_count_total_fareast") or 0)
                    ),
                    "orders_count_total_ural": int(
                        float(row.get("orders_count_total_ural") or 0)
                    ),
                    "orders_count_local_central": int(
                        float(row.get("orders_count_local_central") or 0)
                    ),
                    "orders_count_local_northwest": int(
                        float(row.get("orders_count_local_northwest") or 0)
                    ),
                    "orders_count_local_south_caucasus": int(
                        float(row.get("orders_count_local_south_caucasus") or 0)
                    ),
                    "orders_count_local_volga": int(
                        float(row.get("orders_count_local_volga") or 0)
                    ),
                    "orders_count_local_fareast": int(
                        float(row.get("orders_count_local_fareast") or 0)
                    ),
                    "orders_count_local_ural": int(
                        float(row.get("orders_count_local_ural") or 0)
                    ),
                }

                conn.execute(
                    """
                    INSERT INTO daily_localization(
                      spreadsheet_id, dt, nm_id, orders_count_total, orders_count_local, localization_percent,
                      orders_count_total_central, orders_count_total_northwest, orders_count_total_south_caucasus,
                      orders_count_total_volga, orders_count_total_fareast, orders_count_total_ural,
                      orders_count_local_central, orders_count_local_northwest, orders_count_local_south_caucasus,
                      orders_count_local_volga, orders_count_local_fareast, orders_count_local_ural,
                      source, raw_json, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(spreadsheet_id, dt, nm_id) DO UPDATE SET
                      orders_count_total=excluded.orders_count_total,
                      orders_count_local=excluded.orders_count_local,
                      localization_percent=excluded.localization_percent,
                      orders_count_total_central=excluded.orders_count_total_central,
                      orders_count_total_northwest=excluded.orders_count_total_northwest,
                      orders_count_total_south_caucasus=excluded.orders_count_total_south_caucasus,
                      orders_count_total_volga=excluded.orders_count_total_volga,
                      orders_count_total_fareast=excluded.orders_count_total_fareast,
                      orders_count_total_ural=excluded.orders_count_total_ural,
                      orders_count_local_central=excluded.orders_count_local_central,
                      orders_count_local_northwest=excluded.orders_count_local_northwest,
                      orders_count_local_south_caucasus=excluded.orders_count_local_south_caucasus,
                      orders_count_local_volga=excluded.orders_count_local_volga,
                      orders_count_local_fareast=excluded.orders_count_local_fareast,
                      orders_count_local_ural=excluded.orders_count_local_ural,
                      source=excluded.source,
                      raw_json=excluded.raw_json,
                      updated_at=excluded.updated_at
                    """,
                    (
                        spreadsheet_id,
                        day,
                        nm_id,
                        orders_count_total,
                        orders_count_local,
                        localization_percent,
                        values["orders_count_total_central"],
                        values["orders_count_total_northwest"],
                        values["orders_count_total_south_caucasus"],
                        values["orders_count_total_volga"],
                        values["orders_count_total_fareast"],
                        values["orders_count_total_ural"],
                        values["orders_count_local_central"],
                        values["orders_count_local_northwest"],
                        values["orders_count_local_south_caucasus"],
                        values["orders_count_local_volga"],
                        values["orders_count_local_fareast"],
                        values["orders_count_local_ural"],
                        str(row.get("source") or source),
                        json.dumps(row, ensure_ascii=False),
                        now,
                        now,
                    ),
                )
                affected += 1
        return affected

    def get_daily_localization(
        self,
        spreadsheet_id: str,
        date_from: str,
        date_to: str,
        nm_ids: Optional[List[int]] = None,
    ) -> List[Dict[str, Any]]:
        sql = """
            SELECT
              dt, nm_id, orders_count_total, orders_count_local, localization_percent,
              orders_count_total_central, orders_count_total_northwest, orders_count_total_south_caucasus,
              orders_count_total_volga, orders_count_total_fareast, orders_count_total_ural,
              orders_count_local_central, orders_count_local_northwest, orders_count_local_south_caucasus,
              orders_count_local_volga, orders_count_local_fareast, orders_count_local_ural,
              source
            FROM daily_localization
            WHERE spreadsheet_id = ?
              AND dt >= ?
              AND dt <= ?
        """
        args: List[Any] = [spreadsheet_id, str(date_from)[:10], str(date_to)[:10]]
        if nm_ids:
            marks = ",".join("?" for _ in nm_ids)
            sql += f" AND nm_id IN ({marks})"
            args.extend([int(x) for x in nm_ids])
        sql += " ORDER BY dt ASC, nm_id ASC"
        with self._connect() as conn:
            rows = conn.execute(sql, args).fetchall()
        out: List[Dict[str, Any]] = []
        for row in rows:
            out.append(
                {
                    "date": str(row["dt"] or ""),
                    "nm_id": int(row["nm_id"]),
                    "orders_count_total": int(row["orders_count_total"] or 0),
                    "orders_count_local": int(row["orders_count_local"] or 0),
                    "localization_percent": float(row["localization_percent"] or 0.0),
                    "orders_count_total_central": int(
                        row["orders_count_total_central"] or 0
                    ),
                    "orders_count_total_northwest": int(
                        row["orders_count_total_northwest"] or 0
                    ),
                    "orders_count_total_south_caucasus": int(
                        row["orders_count_total_south_caucasus"] or 0
                    ),
                    "orders_count_total_volga": int(
                        row["orders_count_total_volga"] or 0
                    ),
                    "orders_count_total_fareast": int(
                        row["orders_count_total_fareast"] or 0
                    ),
                    "orders_count_total_ural": int(row["orders_count_total_ural"] or 0),
                    "orders_count_local_central": int(
                        row["orders_count_local_central"] or 0
                    ),
                    "orders_count_local_northwest": int(
                        row["orders_count_local_northwest"] or 0
                    ),
                    "orders_count_local_south_caucasus": int(
                        row["orders_count_local_south_caucasus"] or 0
                    ),
                    "orders_count_local_volga": int(
                        row["orders_count_local_volga"] or 0
                    ),
                    "orders_count_local_fareast": int(
                        row["orders_count_local_fareast"] or 0
                    ),
                    "orders_count_local_ural": int(row["orders_count_local_ural"] or 0),
                    "source": str(row["source"] or ""),
                }
            )
        return out

    def add_audit_event(
        self,
        event_type: str,
        actor: str,
        role: str,
        method: str,
        path: str,
        status_code: int,
        ip: str,
        payload: Dict[str, Any],
    ) -> None:
        now = _now_iso()
        raw_payload = payload if isinstance(payload, dict) else {}
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO audit_events(
                  event_type,
                  actor,
                  role,
                  method,
                  path,
                  status_code,
                  ip,
                  payload_json,
                  created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(event_type or ""),
                    str(actor or ""),
                    str(role or ""),
                    str(method or ""),
                    str(path or ""),
                    int(status_code or 0),
                    str(ip or ""),
                    json.dumps(raw_payload, ensure_ascii=False),
                    now,
                ),
            )
