import io
import json
import gc
import os
import sqlite3
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from typing import Any


BACKEND_COMPAT_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_COMPAT_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_COMPAT_DIR))

import datasets  # noqa: E402
import server  # noqa: E402
import storage  # noqa: E402


class _AuditStorage:
    def __init__(self) -> None:
        self.events = []

    def add_audit_event(self, **kwargs):
        self.events.append(kwargs)

    def is_spreadsheet_active(self, spreadsheet_id: str) -> bool:
        return bool(str(spreadsheet_id or "").strip())


class ApiSecurityTests(unittest.TestCase):
    def setUp(self) -> None:
        self._old_api_key = server.API_KEY
        self._old_bearer = dict(server.BEARER_TOKENS)
        self._old_admin_limit = server.RATE_LIMIT_ADMIN_PER_WINDOW
        self._old_window = server.RATE_LIMIT_WINDOW_SECONDS
        server.ApiHandler._rate_hits = {}

    def tearDown(self) -> None:
        server.API_KEY = self._old_api_key
        server.BEARER_TOKENS = self._old_bearer
        server.RATE_LIMIT_ADMIN_PER_WINDOW = self._old_admin_limit
        server.RATE_LIMIT_WINDOW_SECONDS = self._old_window
        server.ApiHandler._rate_hits = {}

    def _handler(self, headers=None):
        inst = server.ApiHandler.__new__(server.ApiHandler)
        setattr(inst, "headers", headers or {})
        setattr(inst, "client_address", ("127.0.0.1", 12345))
        setattr(inst, "command", "POST")
        setattr(inst, "storage", _AuditStorage())
        setattr(inst, "wfile", io.BytesIO())

        def _send_response(code: int, message: Any = None) -> None:
            del message
            setattr(inst, "_status", code)

        def _send_header(keyword: str, value: str) -> None:
            del keyword, value

        def _end_headers() -> None:
            return None

        setattr(inst, "send_response", _send_response)
        setattr(inst, "send_header", _send_header)
        setattr(inst, "end_headers", _end_headers)
        return inst

    def test_auth_context_accepts_bearer_and_api_key(self):
        server.API_KEY = "root-key"
        server.BEARER_TOKENS = {
            "op-token": {"role": "operator", "actor": "ops-bot"},
        }

        api_key_auth = self._handler({"X-Api-Key": "root-key"})._auth_context()
        self.assertTrue(api_key_auth.ok)
        self.assertEqual(api_key_auth.role, "admin")

        bearer_auth = self._handler(
            {"Authorization": "Bearer op-token"}
        )._auth_context()
        self.assertTrue(bearer_auth.ok)
        self.assertEqual(bearer_auth.role, "operator")
        self.assertEqual(bearer_auth.actor, "ops-bot")

        denied = self._handler({"Authorization": "Bearer nope"})._auth_context()
        self.assertFalse(denied.ok)

    def test_access_and_rate_limit_for_admin_paths(self):
        handler = self._handler()
        auth_client = server.AuthContext(
            ok=True,
            actor="u",
            role="client",
            spreadsheet_ids=set(),
            message="",
        )
        access_ok, msg = handler._check_access("/admin/wb/tokens/list", auth_client)
        self.assertFalse(access_ok)
        self.assertEqual(msg, "forbidden")

        server.RATE_LIMIT_WINDOW_SECONDS = 60
        server.RATE_LIMIT_ADMIN_PER_WINDOW = 2
        auth_admin = server.AuthContext(
            ok=True,
            actor="root",
            role="admin",
            spreadsheet_ids={"*"},
            message="",
        )
        self.assertEqual(
            handler._check_rate_limit("/admin/wb/tokens/list", auth_admin), (True, 2)
        )
        self.assertEqual(
            handler._check_rate_limit("/admin/wb/tokens/list", auth_admin), (True, 2)
        )
        self.assertEqual(
            handler._check_rate_limit("/admin/wb/tokens/list", auth_admin), (False, 2)
        )

    def test_auth_context_denies_when_auth_is_not_configured(self):
        server.API_KEY = ""
        server.BEARER_TOKENS = {}

        auth = self._handler()._auth_context()
        self.assertFalse(auth.ok)
        self.assertIn("not configured", auth.message)

    def test_spreadsheet_scope_blocks_cross_tenant_for_client(self):
        server.API_KEY = ""
        server.BEARER_TOKENS = {
            "client-token": {
                "role": "client",
                "actor": "client-1",
                "spreadsheets": ["ss-1"],
            }
        }

        handler = self._handler({"Authorization": "Bearer client-token"})
        auth = handler._auth_context()

        allow_ok, allow_msg = handler._check_spreadsheet_scope(
            "/ss/wb/token/get",
            {"spreadsheet_id": "ss-1"},
            auth,
        )
        self.assertTrue(allow_ok)
        self.assertEqual(allow_msg, "")

        deny_ok, deny_msg = handler._check_spreadsheet_scope(
            "/ss/wb/token/get",
            {"spreadsheet_id": "ss-2"},
            auth,
        )
        self.assertFalse(deny_ok)
        self.assertEqual(deny_msg, "forbidden")

    def test_audit_payload_masks_sensitive_keys(self):
        handler = self._handler()
        auth = server.AuthContext(
            ok=True,
            actor="ops",
            role="operator",
            spreadsheet_ids={"*"},
            message="",
        )
        payload = {
            "token": "raw-token",
            "nested": {"password": "p", "keep": 1},
            "items": [{"api_key": "k1"}, {"secret": "k2"}],
        }
        handler._set_audit_context("/admin/wb/tokens/add", auth, payload)
        handler._send(200, {"ok": True})

        audit_storage = getattr(handler, "storage")
        self.assertEqual(len(audit_storage.events), 1)
        logged = audit_storage.events[0]["payload"]
        self.assertEqual(logged["token"], "***")
        self.assertEqual(logged["nested"]["password"], "***")
        self.assertEqual(logged["items"][0]["api_key"], "***")
        self.assertEqual(logged["items"][1]["secret"], "***")


class StorageTokenEncryptionTests(unittest.TestCase):
    def setUp(self) -> None:
        self._old_key = os.environ.get("BTLZ_TOKEN_ENCRYPTION_KEY")

    def tearDown(self) -> None:
        if self._old_key is None:
            os.environ.pop("BTLZ_TOKEN_ENCRYPTION_KEY", None)
        else:
            os.environ["BTLZ_TOKEN_ENCRYPTION_KEY"] = self._old_key

    def test_tokens_work_without_encryption_key(self):
        os.environ.pop("BTLZ_TOKEN_ENCRYPTION_KEY", None)
        fd, db_path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        try:
            db = storage.Storage(db_path)
            db.add_wb_token("ss-1", "token-plain", None)
            items = db.list_wb_tokens("ss-1")
            self.assertEqual(len(items), 1)
            self.assertEqual(items[0].token, "token-plain")

            with sqlite3.connect(db_path) as conn:
                raw = conn.execute(
                    "SELECT token FROM wb_tokens WHERE spreadsheet_id = ?",
                    ("ss-1",),
                ).fetchone()[0]
            self.assertEqual(raw, "token-plain")
        finally:
            gc.collect()


class CollectorLockTests(unittest.TestCase):
    def test_collector_lock_conflict_and_release(self):
        fd, db_path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        try:
            db = storage.Storage(db_path)

            acquired_1, state_1 = db.acquire_collector_lock(
                owner="owner-1",
                ttl_seconds=120,
            )
            self.assertTrue(acquired_1)
            self.assertEqual(state_1.get("owner"), "owner-1")

            acquired_2, state_2 = db.acquire_collector_lock(
                owner="owner-2",
                ttl_seconds=120,
            )
            self.assertFalse(acquired_2)
            self.assertEqual(state_2.get("owner"), "owner-1")

            db.release_collector_lock("owner-1")

            acquired_3, state_3 = db.acquire_collector_lock(
                owner="owner-2",
                ttl_seconds=120,
            )
            self.assertTrue(acquired_3)
            self.assertEqual(state_3.get("owner"), "owner-2")
        finally:
            gc.collect()

    def test_collector_lock_parallel_acquire_single_winner(self):
        fd, db_path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        try:
            db_a = storage.Storage(db_path)
            db_b = storage.Storage(db_path)
            barrier = threading.Barrier(3)
            results = []

            def _worker(db: storage.Storage, owner: str) -> None:
                barrier.wait()
                ok, state = db.acquire_collector_lock(owner=owner, ttl_seconds=120)
                results.append((owner, ok, state))

            t1 = threading.Thread(target=_worker, args=(db_a, "owner-A"))
            t2 = threading.Thread(target=_worker, args=(db_b, "owner-B"))
            t1.start()
            t2.start()
            barrier.wait()
            t1.join()
            t2.join()

            success_count = sum(1 for _, ok, _ in results if ok)
            self.assertEqual(success_count, 1)

            winner = next(owner for owner, ok, _ in results if ok)
            self.assertTrue(db_a.refresh_collector_lock(owner=winner, ttl_seconds=120))
            loser = "owner-A" if winner == "owner-B" else "owner-B"
            self.assertFalse(db_a.refresh_collector_lock(owner=loser, ttl_seconds=120))
        finally:
            gc.collect()

    def test_tokens_reencrypt_existing_plain_rows_when_key_is_enabled(self):
        try:
            from cryptography.fernet import Fernet
        except Exception:
            self.skipTest("cryptography is not installed")

        fd, db_path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        try:
            os.environ.pop("BTLZ_TOKEN_ENCRYPTION_KEY", None)
            plain_db = storage.Storage(db_path)
            plain_db.add_wb_token("ss-1", "token-legacy", "sid-1")

            os.environ["BTLZ_TOKEN_ENCRYPTION_KEY"] = Fernet.generate_key().decode(
                "utf-8"
            )
            enc_db = storage.Storage(db_path)
            items = enc_db.list_wb_tokens("ss-1")

            self.assertEqual(len(items), 1)
            self.assertEqual(items[0].token, "token-legacy")

            with sqlite3.connect(db_path) as conn:
                raw = conn.execute(
                    "SELECT token FROM wb_tokens WHERE spreadsheet_id = ?",
                    ("ss-1",),
                ).fetchone()[0]
            self.assertTrue(raw.startswith(storage.TOKEN_ENC_PREFIX))
        finally:
            gc.collect()


class QualityGateRoutingTests(unittest.TestCase):
    def setUp(self) -> None:
        self._orig_enabled = datasets.QUALITY_GATES_ENABLED
        self._orig_default = datasets.QUALITY_GATES_DEFAULT_UPSTREAM
        self._orig_file = datasets.QUALITY_GATES_FILE
        datasets._QUALITY_GATES_CACHE_MTIME = None
        datasets._QUALITY_GATES_CACHE = {}

    def tearDown(self) -> None:
        datasets.QUALITY_GATES_ENABLED = self._orig_enabled
        datasets.QUALITY_GATES_DEFAULT_UPSTREAM = self._orig_default
        datasets.QUALITY_GATES_FILE = self._orig_file
        datasets._QUALITY_GATES_CACHE_MTIME = None
        datasets._QUALITY_GATES_CACHE = {}

    def test_quality_gate_modes_and_threshold_logic(self):
        with tempfile.NamedTemporaryFile(
            "w", suffix=".json", delete=False, encoding="utf-8"
        ) as fh:
            json.dump(
                {
                    "datasets": {
                        "wbCardsData_v1": "local",
                        "wb10xAnalyticsData_v1": "upstream",
                        "wb10xSalesReport_v1": {
                            "exact_ratio": 0.87,
                            "min_exact_ratio": 0.90,
                        },
                    }
                },
                fh,
            )
            quality_path = fh.name

        try:
            datasets.QUALITY_GATES_ENABLED = True
            datasets.QUALITY_GATES_DEFAULT_UPSTREAM = False
            datasets.QUALITY_GATES_FILE = quality_path
            datasets._QUALITY_GATES_CACHE_MTIME = None
            datasets._QUALITY_GATES_CACHE = {}

            self.assertFalse(datasets._should_try_upstream("wbCardsData_v1"))
            self.assertTrue(datasets._should_try_upstream("wb10xAnalyticsData_v1"))
            self.assertTrue(datasets._should_try_upstream("wb10xSalesReport_v1"))
            self.assertFalse(datasets._should_try_upstream("unknown_dataset"))
        finally:
            os.unlink(quality_path)

    def test_quality_gates_disabled_means_try_upstream(self):
        datasets.QUALITY_GATES_ENABLED = False
        self.assertTrue(datasets._should_try_upstream("anything"))


if __name__ == "__main__":
    unittest.main()
