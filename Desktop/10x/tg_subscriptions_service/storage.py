import os
import sqlite3
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from config import PlanConfig


UTC = timezone.utc


def _now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _parse_iso(value: str) -> datetime:
    text = str(value or "").strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    return datetime.fromisoformat(text)


@dataclass
class ActiveSubscription:
    plan_code: str
    starts_at: str
    ends_at: str
    status: str


class Storage:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._lock = threading.Lock()
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
                CREATE TABLE IF NOT EXISTS customers (
                  telegram_user_id INTEGER PRIMARY KEY,
                  username TEXT,
                  first_name TEXT,
                  last_name TEXT,
                  language_code TEXT,
                  status TEXT NOT NULL DEFAULT 'active',
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS plans (
                  code TEXT PRIMARY KEY,
                  title TEXT NOT NULL,
                  description TEXT NOT NULL,
                  amount_int INTEGER NOT NULL,
                  currency TEXT NOT NULL,
                  period_days INTEGER NOT NULL,
                  is_active INTEGER NOT NULL DEFAULT 1,
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS payments (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  telegram_user_id INTEGER NOT NULL,
                  plan_code TEXT NOT NULL,
                  payload TEXT NOT NULL UNIQUE,
                  telegram_payment_charge_id TEXT UNIQUE,
                  provider_payment_charge_id TEXT,
                  amount_int INTEGER NOT NULL,
                  currency TEXT NOT NULL,
                  status TEXT NOT NULL,
                  raw_json TEXT NOT NULL DEFAULT '{}',
                  subscription_id INTEGER,
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS subscriptions (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  telegram_user_id INTEGER NOT NULL,
                  plan_code TEXT NOT NULL,
                  payment_id INTEGER,
                  status TEXT NOT NULL,
                  starts_at TEXT NOT NULL,
                  ends_at TEXT NOT NULL,
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_subscriptions_user_status
                  ON subscriptions (telegram_user_id, status, ends_at);

                CREATE TABLE IF NOT EXISTS billing_events (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  event_type TEXT NOT NULL,
                  telegram_user_id INTEGER,
                  payload TEXT,
                  raw_json TEXT NOT NULL DEFAULT '{}',
                  created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS onboarding_profiles (
                  telegram_user_id INTEGER PRIMARY KEY,
                  email TEXT NOT NULL,
                  spreadsheet_id TEXT NOT NULL,
                  last_status TEXT NOT NULL DEFAULT 'pending',
                  last_error TEXT,
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS processed_updates (
                  update_id INTEGER PRIMARY KEY,
                  processed_at TEXT NOT NULL
                );
                """
            )

    def sync_plans(self, plans: List[PlanConfig]) -> None:
        now = _now_iso()
        with self._lock, self._connect() as conn:
            for plan in plans:
                conn.execute(
                    """
                    INSERT INTO plans (code, title, description, amount_int, currency, period_days, is_active, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?)
                    ON CONFLICT(code) DO UPDATE SET
                      title=excluded.title,
                      description=excluded.description,
                      amount_int=excluded.amount_int,
                      currency=excluded.currency,
                      period_days=excluded.period_days,
                      is_active=1,
                      updated_at=excluded.updated_at
                    """,
                    (
                        plan.code,
                        plan.title,
                        plan.description,
                        plan.amount_int,
                        plan.currency,
                        plan.period_days,
                        now,
                        now,
                    ),
                )

    def upsert_customer(self, user: Dict[str, Any]) -> int:
        user_id = int(user.get("id") or 0)
        if user_id <= 0:
            raise ValueError("telegram user id is required")
        now = _now_iso()
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO customers (telegram_user_id, username, first_name, last_name, language_code, status, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, 'active', ?, ?)
                ON CONFLICT(telegram_user_id) DO UPDATE SET
                  username=excluded.username,
                  first_name=excluded.first_name,
                  last_name=excluded.last_name,
                  language_code=excluded.language_code,
                  updated_at=excluded.updated_at
                """,
                (
                    user_id,
                    str(user.get("username") or ""),
                    str(user.get("first_name") or ""),
                    str(user.get("last_name") or ""),
                    str(user.get("language_code") or ""),
                    now,
                    now,
                ),
            )
        return user_id

    def list_active_plans(self) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT code, title, description, amount_int, currency, period_days
                FROM plans
                WHERE is_active = 1
                ORDER BY amount_int ASC, code ASC
                """
            ).fetchall()
        return [dict(row) for row in rows]

    def get_plan(self, plan_code: str) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT code, title, description, amount_int, currency, period_days, is_active
                FROM plans
                WHERE code = ?
                """,
                (plan_code,),
            ).fetchone()
        return dict(row) if row else None

    def create_payment_intent(
        self,
        telegram_user_id: int,
        plan_code: str,
        payload: str,
        amount_int: int,
        currency: str,
        raw_json: str,
    ) -> None:
        now = _now_iso()
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO payments (
                  telegram_user_id,
                  plan_code,
                  payload,
                  amount_int,
                  currency,
                  status,
                  raw_json,
                  created_at,
                  updated_at
                ) VALUES (?, ?, ?, ?, ?, 'pending', ?, ?, ?)
                ON CONFLICT(payload) DO UPDATE SET
                  telegram_user_id=excluded.telegram_user_id,
                  plan_code=excluded.plan_code,
                  amount_int=excluded.amount_int,
                  currency=excluded.currency,
                  status='pending',
                  raw_json=excluded.raw_json,
                  updated_at=excluded.updated_at
                """,
                (
                    telegram_user_id,
                    plan_code,
                    payload,
                    amount_int,
                    currency,
                    raw_json,
                    now,
                    now,
                ),
            )

    def get_payment_by_payload(self, payload: str) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT *
                FROM payments
                WHERE payload = ?
                """,
                (payload,),
            ).fetchone()
        return dict(row) if row else None

    def activate_subscription_from_payment(
        self,
        telegram_user_id: int,
        payload: str,
        telegram_payment_charge_id: str,
        provider_payment_charge_id: str,
        amount_int: int,
        currency: str,
        raw_json: str,
    ) -> ActiveSubscription:
        now_dt = datetime.now(UTC)
        now_iso = now_dt.isoformat(timespec="seconds").replace("+00:00", "Z")

        with self._lock, self._connect() as conn:
            row = conn.execute(
                """
                SELECT p.*, s.plan_code AS sub_plan_code, s.starts_at AS sub_starts_at, s.ends_at AS sub_ends_at, s.status AS sub_status
                FROM payments p
                LEFT JOIN subscriptions s ON s.id = p.subscription_id
                WHERE p.telegram_payment_charge_id = ?
                """,
                (telegram_payment_charge_id,),
            ).fetchone()
            if row and row["status"] == "paid" and row["sub_ends_at"]:
                return ActiveSubscription(
                    plan_code=str(row["sub_plan_code"]),
                    starts_at=str(row["sub_starts_at"]),
                    ends_at=str(row["sub_ends_at"]),
                    status=str(row["sub_status"] or "active"),
                )

            payment = conn.execute(
                "SELECT * FROM payments WHERE payload = ?",
                (payload,),
            ).fetchone()

            if payment is None:
                raise ValueError("payment intent not found for payload")

            payment_user_id = int(payment["telegram_user_id"] or 0)
            if payment_user_id != int(telegram_user_id):
                raise ValueError("payment intent user mismatch")

            plan_code = str(payment["plan_code"])
            plan = conn.execute(
                "SELECT period_days FROM plans WHERE code = ?",
                (plan_code,),
            ).fetchone()
            period_days = int(plan["period_days"]) if plan else 30
            period_days = max(period_days, 1)

            latest_active = conn.execute(
                """
                SELECT *
                FROM subscriptions
                WHERE telegram_user_id = ? AND status = 'active'
                ORDER BY ends_at DESC
                LIMIT 1
                """,
                (telegram_user_id,),
            ).fetchone()

            start_dt = now_dt
            if latest_active is not None:
                latest_end = _parse_iso(str(latest_active["ends_at"]))
                if latest_end > now_dt:
                    start_dt = latest_end

            end_dt = start_dt + timedelta(days=period_days)
            starts_at = start_dt.isoformat(timespec="seconds").replace("+00:00", "Z")
            ends_at = end_dt.isoformat(timespec="seconds").replace("+00:00", "Z")

            conn.execute(
                """
                INSERT INTO subscriptions (
                  telegram_user_id,
                  plan_code,
                  payment_id,
                  status,
                  starts_at,
                  ends_at,
                  created_at,
                  updated_at
                ) VALUES (?, ?, ?, 'active', ?, ?, ?, ?)
                """,
                (
                    telegram_user_id,
                    plan_code,
                    int(payment["id"]),
                    starts_at,
                    ends_at,
                    now_iso,
                    now_iso,
                ),
            )
            sub_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]

            conn.execute(
                """
                UPDATE payments
                SET telegram_payment_charge_id = ?,
                    provider_payment_charge_id = ?,
                    amount_int = ?,
                    currency = ?,
                    status = 'paid',
                    raw_json = ?,
                    subscription_id = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    telegram_payment_charge_id,
                    provider_payment_charge_id,
                    amount_int,
                    currency,
                    raw_json,
                    sub_id,
                    now_iso,
                    int(payment["id"]),
                ),
            )

            return ActiveSubscription(
                plan_code=plan_code,
                starts_at=starts_at,
                ends_at=ends_at,
                status="active",
            )

    def get_active_subscription(
        self, telegram_user_id: int
    ) -> Optional[ActiveSubscription]:
        now_iso = _now_iso()
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT plan_code, starts_at, ends_at, status
                FROM subscriptions
                WHERE telegram_user_id = ?
                  AND status = 'active'
                  AND ends_at > ?
                ORDER BY ends_at DESC
                LIMIT 1
                """,
                (telegram_user_id, now_iso),
            ).fetchone()
        if not row:
            return None
        return ActiveSubscription(
            plan_code=str(row["plan_code"]),
            starts_at=str(row["starts_at"]),
            ends_at=str(row["ends_at"]),
            status=str(row["status"]),
        )

    def expire_due_subscriptions(self) -> int:
        now_iso = _now_iso()
        with self._lock, self._connect() as conn:
            cur = conn.execute(
                """
                UPDATE subscriptions
                SET status = 'expired',
                    updated_at = ?
                WHERE status = 'active' AND ends_at <= ?
                """,
                (now_iso, now_iso),
            )
            return int(cur.rowcount or 0)

    def upsert_onboarding_profile(
        self,
        telegram_user_id: int,
        email: str,
        spreadsheet_id: str,
    ) -> None:
        now = _now_iso()
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO onboarding_profiles (
                  telegram_user_id,
                  email,
                  spreadsheet_id,
                  last_status,
                  last_error,
                  created_at,
                  updated_at
                ) VALUES (?, ?, ?, 'pending', NULL, ?, ?)
                ON CONFLICT(telegram_user_id) DO UPDATE SET
                  email = excluded.email,
                  spreadsheet_id = excluded.spreadsheet_id,
                  last_status = 'pending',
                  last_error = NULL,
                  updated_at = excluded.updated_at
                """,
                (telegram_user_id, email, spreadsheet_id, now, now),
            )

    def get_onboarding_profile(self, telegram_user_id: int) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM onboarding_profiles WHERE telegram_user_id = ?",
                (telegram_user_id,),
            ).fetchone()
        return dict(row) if row else None

    def set_onboarding_result(
        self,
        telegram_user_id: int,
        ok: bool,
        error_text: str = "",
    ) -> None:
        now = _now_iso()
        status = "active" if ok else "failed"
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                UPDATE onboarding_profiles
                SET last_status = ?,
                    last_error = ?,
                    updated_at = ?
                WHERE telegram_user_id = ?
                """,
                (status, error_text or None, now, telegram_user_id),
            )

    def add_billing_event(
        self,
        event_type: str,
        telegram_user_id: Optional[int],
        payload: str,
        raw_json: str,
    ) -> None:
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO billing_events (event_type, telegram_user_id, payload, raw_json, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (event_type, telegram_user_id, payload, raw_json, _now_iso()),
            )

    def register_webhook_update(self, update_id: int) -> bool:
        value = int(update_id or 0)
        if value <= 0:
            return True
        with self._lock, self._connect() as conn:
            cur = conn.execute(
                """
                INSERT OR IGNORE INTO processed_updates (update_id, processed_at)
                VALUES (?, ?)
                """,
                (value, _now_iso()),
            )
            return bool(int(cur.rowcount or 0) > 0)

    def prune_processed_updates(self, retention_days: int) -> int:
        days = max(int(retention_days or 1), 1)
        cutoff = (
            (datetime.now(UTC) - timedelta(days=days))
            .isoformat(timespec="seconds")
            .replace("+00:00", "Z")
        )
        with self._lock, self._connect() as conn:
            cur = conn.execute(
                """
                DELETE FROM processed_updates
                WHERE processed_at < ?
                """,
                (cutoff,),
            )
            return int(cur.rowcount or 0)
