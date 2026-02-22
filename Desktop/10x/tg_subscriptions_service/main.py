import json
import threading
import time
import traceback
import urllib.parse
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, Optional, Tuple
from uuid import uuid4

from config import Settings, load_settings
from onboarding_bridge import OnboardingBridge
from storage import Storage
from telegram_api import TelegramApi, TelegramApiError


UTC = timezone.utc


def _now_utc_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _as_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False)


class SubscriptionService:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.storage = Storage(settings.db_path)
        self.storage.sync_plans(settings.plans)
        self.telegram = TelegramApi(
            bot_token=settings.bot_token,
            timeout_seconds=settings.request_timeout_seconds,
            request_retries=settings.request_retries,
        )
        self.onboarding = OnboardingBridge(
            base_url=settings.backend_api_base,
            api_key=settings.backend_api_key,
            bearer_token=settings.backend_bearer_token,
            timeout_seconds=settings.request_timeout_seconds,
            request_retries=settings.request_retries,
        )
        self._maintenance_stop = threading.Event()
        self._maintenance_thread = threading.Thread(
            target=self._maintenance_loop,
            name="tg-svc-maintenance",
            daemon=True,
        )
        self._metrics_lock = threading.Lock()
        self._metrics: Dict[str, int] = {
            "tg_updates_total": 0,
            "tg_webhook_auth_failures_total": 0,
            "tg_webhook_duplicates_total": 0,
            "tg_webhook_dedupe_pruned_total": 0,
            "tg_onboarding_success_total": 0,
            "tg_onboarding_failures_total": 0,
            "tg_onboarding_unpaid_rejections_total": 0,
            "tg_admin_auth_failures_total": 0,
            "tg_admin_wb_token_success_total": 0,
            "tg_admin_wb_token_failures_total": 0,
        }
        self._last_dedupe_cleanup_ts = 0.0

    def _metric_inc(self, name: str, value: int = 1) -> None:
        key = str(name or "").strip()
        if not key:
            return
        delta = int(value or 0)
        if delta == 0:
            return
        with self._metrics_lock:
            self._metrics[key] = int(self._metrics.get(key, 0)) + delta

    def metrics_snapshot(self) -> Dict[str, int]:
        with self._metrics_lock:
            return {k: int(v) for k, v in self._metrics.items()}

    def start(self) -> None:
        if self.settings.auto_set_webhook and self.settings.webhook_base_url:
            webhook_url = (
                self.settings.webhook_base_url.rstrip("/") + self.settings.webhook_path
            )
            try:
                self.telegram.set_webhook(webhook_url, self.settings.webhook_secret)
                print(f"[tg-subscriptions] webhook set: {webhook_url}")
            except Exception as exc:
                print(f"[tg-subscriptions] webhook setup failed: {exc}")

        self._maintenance_thread.start()

    def stop(self) -> None:
        self._maintenance_stop.set()
        self._maintenance_thread.join(timeout=2)

    def _maintenance_loop(self) -> None:
        while not self._maintenance_stop.is_set():
            try:
                expired_count = self.storage.expire_due_subscriptions()
                if expired_count > 0:
                    print(f"[tg-subscriptions] expired subscriptions: {expired_count}")

                now_ts = time.time()
                if now_ts - self._last_dedupe_cleanup_ts >= 3600:
                    deleted = self.storage.prune_processed_updates(
                        self.settings.webhook_dedupe_retention_days
                    )
                    if deleted > 0:
                        self._metric_inc("tg_webhook_dedupe_pruned_total", deleted)
                    self._last_dedupe_cleanup_ts = now_ts
            except Exception as exc:
                print(f"[tg-subscriptions] maintenance error: {exc}")
            self._maintenance_stop.wait(60)

    def handle_update(self, update: Dict[str, Any]) -> None:
        if not isinstance(update, dict):
            return

        update_id = 0
        try:
            update_id = int(update.get("update_id") or 0)
        except (TypeError, ValueError):
            update_id = 0
        if update_id > 0 and not self.storage.register_webhook_update(update_id):
            self._metric_inc("tg_webhook_duplicates_total")
            return

        self._metric_inc("tg_updates_total")

        if isinstance(update.get("pre_checkout_query"), dict):
            self._handle_pre_checkout_query(update["pre_checkout_query"])
            return

        message = update.get("message")
        if not isinstance(message, dict):
            return

        successful_payment = message.get("successful_payment")
        if isinstance(successful_payment, dict):
            self._handle_successful_payment(message, successful_payment)
            return

        text = str(message.get("text") or "").strip()
        if not text:
            return

        command, args = self._parse_command(text)
        if not command:
            return

        from_user = message.get("from") or {}
        chat = message.get("chat") or {}
        chat_id = int(chat.get("id") or 0)
        user_id = self.storage.upsert_customer(
            from_user if isinstance(from_user, dict) else {}
        )

        if chat_id <= 0 or user_id <= 0:
            return

        if command == "start":
            self._cmd_start(chat_id)
            return
        if command == "plans":
            self._cmd_plans(chat_id)
            return
        if command == "buy":
            self._cmd_buy(chat_id, user_id, args)
            return
        if command == "status":
            self._cmd_status(chat_id, user_id)
            return
        if command == "support":
            self._cmd_support(chat_id)
            return
        if command == "onboard":
            self._cmd_onboard(chat_id, user_id, args)
            return

        self._safe_send_message(
            chat_id,
            "Unknown command. Available: /start, /plans, /buy, /status, /support, /onboard",
        )

    def _parse_command(self, text: str) -> Tuple[str, str]:
        if not text.startswith("/"):
            return "", ""
        head, *tail = text.split(maxsplit=1)
        cmd = head[1:]
        if "@" in cmd:
            cmd = cmd.split("@", 1)[0]
        cmd = cmd.strip().lower()
        args = tail[0].strip() if tail else ""
        return cmd, args

    def _cmd_start(self, chat_id: int) -> None:
        text = (
            "Welcome!\\n"
            "I can help you buy and manage your subscription.\\n\\n"
            "Commands:\\n"
            "/plans - list plans\\n"
            "/buy <plan_code> - buy subscription\\n"
            "/status - show your subscription status\\n"
            "/onboard <email> <spreadsheet_id> - connect your workspace\\n"
            "/support - contact support"
        )
        self._safe_send_message(chat_id, text)

    def _cmd_plans(self, chat_id: int) -> None:
        plans = self.storage.list_active_plans()
        if not plans:
            self._safe_send_message(
                chat_id, "No active plans right now. Please contact support."
            )
            return
        lines = ["Available plans:"]
        for plan in plans:
            lines.append(
                f"- {plan['code']}: {plan['title']} | {plan['amount_int']} {plan['currency']} | {plan['period_days']} days"
            )
        lines.append("\\nUse: /buy <plan_code>")
        self._safe_send_message(chat_id, "\\n".join(lines))

    def _cmd_buy(self, chat_id: int, user_id: int, args: str) -> None:
        plan_code = str(args or "").strip()
        if not plan_code:
            plans = self.storage.list_active_plans()
            if not plans:
                self._safe_send_message(chat_id, "No active plans. Contact support.")
                return
            plan_code = str(plans[0]["code"])

        plan = self.storage.get_plan(plan_code)
        if not plan or int(plan.get("is_active") or 0) != 1:
            self._safe_send_message(chat_id, f"Plan '{plan_code}' is not available.")
            return

        payload = f"sub:{plan_code}:{user_id}:{uuid4().hex[:12]}"
        self.storage.create_payment_intent(
            telegram_user_id=user_id,
            plan_code=plan_code,
            payload=payload,
            amount_int=int(plan["amount_int"]),
            currency=str(plan["currency"]),
            raw_json=_as_json({"created_at": _now_utc_iso(), "chat_id": chat_id}),
        )

        try:
            self.telegram.send_invoice(
                chat_id=chat_id,
                title=str(plan["title"]),
                description=str(plan["description"]),
                payload_value=payload,
                currency=str(plan["currency"]),
                amount_int=int(plan["amount_int"]),
                period_days=int(plan["period_days"]),
            )
        except TelegramApiError as exc:
            self._safe_send_message(
                chat_id, f"Failed to create invoice. Try again later.\\n{exc}"
            )
            return

        self._safe_send_message(
            chat_id,
            "Invoice sent. Complete payment in Telegram to activate subscription.",
        )

    def _cmd_status(self, chat_id: int, user_id: int) -> None:
        sub = self.storage.get_active_subscription(user_id)
        if not sub:
            self._safe_send_message(
                chat_id, "No active subscription. Use /plans and /buy."
            )
            return
        self._safe_send_message(
            chat_id,
            (
                "Subscription status: active\\n"
                f"Plan: {sub.plan_code}\\n"
                f"Starts: {sub.starts_at}\\n"
                f"Ends: {sub.ends_at}"
            ),
        )

    def _cmd_support(self, chat_id: int) -> None:
        self._safe_send_message(chat_id, self.settings.command_support_text)

    def _cmd_onboard(self, chat_id: int, user_id: int, args: str) -> None:
        if not self.onboarding.enabled():
            self._metric_inc("tg_onboarding_failures_total")
            self._safe_send_message(chat_id, "Onboarding backend is not configured.")
            return

        sub = self.storage.get_active_subscription(user_id)
        if not sub:
            self._metric_inc("tg_onboarding_failures_total")
            self._metric_inc("tg_onboarding_unpaid_rejections_total")
            self._safe_send_message(
                chat_id,
                "No active subscription. Complete payment first with /plans and /buy.",
            )
            return

        chunks = args.split()
        if len(chunks) < 2:
            self._safe_send_message(
                chat_id,
                "Usage: /onboard <email> <spreadsheet_id>",
            )
            return

        email = chunks[0].strip()
        spreadsheet_id = chunks[1].strip()
        if not email or not spreadsheet_id:
            self._safe_send_message(
                chat_id,
                "Invalid args. Usage: /onboard <email> <spreadsheet_id>",
            )
            return

        self.storage.upsert_onboarding_profile(user_id, email, spreadsheet_id)

        try:
            self.onboarding.provision_client(email, spreadsheet_id)
            self.storage.set_onboarding_result(user_id, ok=True)
            self._metric_inc("tg_onboarding_success_total")
        except Exception as exc:
            self.storage.set_onboarding_result(user_id, ok=False, error_text=str(exc))
            self._metric_inc("tg_onboarding_failures_total")
            self._safe_send_message(chat_id, f"Onboarding failed: {exc}")
            return

        self._safe_send_message(
            chat_id, "Onboarding completed. Your workspace is connected."
        )

    def handle_admin_wb_token(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        if not self.onboarding.enabled():
            raise RuntimeError("onboarding backend is not configured")

        spreadsheet_id = str(
            payload.get("spreadsheet_id") or payload.get("ssId") or ""
        ).strip()
        email = str(payload.get("email") or payload.get("owner_email") or "").strip()
        wb_token = str(payload.get("wb_token") or payload.get("token") or "").strip()

        if not spreadsheet_id or not email or not wb_token:
            raise ValueError("spreadsheet_id, email, wb_token are required")

        provision_raw = payload.get("provision_after_add")
        if provision_raw is None:
            provision_after_add = True
        elif isinstance(provision_raw, str):
            provision_after_add = provision_raw.strip().lower() in {
                "1",
                "true",
                "yes",
                "on",
            }
        else:
            provision_after_add = bool(provision_raw)

        result: Dict[str, Any] = {
            "token_add": self.onboarding.add_wb_token(spreadsheet_id, email, wb_token)
        }
        if provision_after_add:
            result["provision"] = self.onboarding.provision_client(
                email, spreadsheet_id
            )
        return result

    def _handle_pre_checkout_query(self, query: Dict[str, Any]) -> None:
        query_id = str(query.get("id") or "")
        payload = str(query.get("invoice_payload") or "")
        from_user = query.get("from") or {}
        telegram_user_id = (
            int(from_user.get("id") or 0) if isinstance(from_user, dict) else 0
        )

        payment = self.storage.get_payment_by_payload(payload)
        ok = bool(payment and payment.get("status") == "pending")
        error_message = "Payment is unavailable. Please create a new invoice with /buy"

        if ok:
            payment_row = payment if isinstance(payment, dict) else {}
            plan = self.storage.get_plan(str(payment_row.get("plan_code") or ""))
            ok = bool(plan and int(plan.get("is_active") or 0) == 1)
            if not ok:
                error_message = "Selected plan is not available anymore"
            else:
                expected_amount = int(payment_row.get("amount_int") or 0)
                expected_currency = str(payment_row.get("currency") or "").upper()
                got_amount = int(query.get("total_amount") or 0)
                got_currency = str(query.get("currency") or "").upper()
                if expected_amount != got_amount or expected_currency != got_currency:
                    ok = False
                    error_message = (
                        "Invoice amount mismatch. Please create a new invoice with /buy"
                    )

        try:
            self.telegram.answer_pre_checkout_query(
                pre_checkout_query_id=query_id,
                ok=ok,
                error_message=error_message if not ok else "",
            )
        except Exception as exc:
            print(f"[tg-subscriptions] pre_checkout answer failed: {exc}")
            return

        self.storage.add_billing_event(
            event_type="pre_checkout_query",
            telegram_user_id=telegram_user_id or None,
            payload=payload,
            raw_json=_as_json(query),
        )

    def _handle_successful_payment(
        self, message: Dict[str, Any], successful_payment: Dict[str, Any]
    ) -> None:
        from_user = message.get("from") or {}
        chat = message.get("chat") or {}
        chat_id = int(chat.get("id") or 0) if isinstance(chat, dict) else 0

        if not isinstance(from_user, dict):
            return
        user_id = self.storage.upsert_customer(from_user)

        payload = str(successful_payment.get("invoice_payload") or "")
        telegram_payment_charge_id = str(
            successful_payment.get("telegram_payment_charge_id") or ""
        )
        provider_payment_charge_id = str(
            successful_payment.get("provider_payment_charge_id") or ""
        )
        amount_int = int(successful_payment.get("total_amount") or 0)
        currency = str(successful_payment.get("currency") or "")

        if not payload or not telegram_payment_charge_id:
            if chat_id > 0:
                self._safe_send_message(
                    chat_id,
                    "Payment received, but payload is invalid. Contact support.",
                )
            return

        try:
            sub = self.storage.activate_subscription_from_payment(
                telegram_user_id=user_id,
                payload=payload,
                telegram_payment_charge_id=telegram_payment_charge_id,
                provider_payment_charge_id=provider_payment_charge_id,
                amount_int=amount_int,
                currency=currency,
                raw_json=_as_json(successful_payment),
            )
        except Exception as exc:
            print(f"[tg-subscriptions] payment activation failed: {exc}")
            traceback.print_exc()
            if chat_id > 0:
                self._safe_send_message(
                    chat_id,
                    "Payment received but activation failed. Support has been notified.",
                )
            return

        self.storage.add_billing_event(
            event_type="successful_payment",
            telegram_user_id=user_id,
            payload=payload,
            raw_json=_as_json(successful_payment),
        )

        if chat_id > 0:
            self._safe_send_message(
                chat_id,
                (
                    "Subscription activated.\\n"
                    f"Plan: {sub.plan_code}\\n"
                    f"Active till: {sub.ends_at}\\n\\n"
                    "Next step: run /onboard <email> <spreadsheet_id>"
                ),
            )

    def _safe_send_message(self, chat_id: int, text: str) -> None:
        try:
            self.telegram.send_message(chat_id=chat_id, text=text)
        except Exception as exc:
            print(f"[tg-subscriptions] send message failed: {exc}")


class WebhookHandler(BaseHTTPRequestHandler):
    service: Optional[SubscriptionService] = None
    settings: Optional[Settings] = None

    def _send_json(self, code: int, payload: Dict[str, Any]) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: Any) -> None:
        print("[tg-subscriptions]", format % args)

    def _read_json_payload(self) -> Dict[str, Any]:
        try:
            content_length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            content_length = 0

        try:
            raw = self.rfile.read(content_length) if content_length > 0 else b"{}"
            payload = json.loads(raw.decode("utf-8")) if raw else {}
            if not isinstance(payload, dict):
                raise ValueError("payload must be object")
            return payload
        except Exception as exc:
            raise ValueError(f"invalid JSON: {exc}") from exc

    def do_GET(self) -> None:
        service = self.service
        settings = self.settings
        path = urllib.parse.urlparse(self.path).path
        if path == "/health":
            self._send_json(HTTPStatus.OK, {"success": True, "status": "ok"})
            return
        if path == "/metrics":
            if service is None or settings is None:
                self._send_json(
                    HTTPStatus.SERVICE_UNAVAILABLE,
                    {"success": False, "message": "service unavailable"},
                )
                return
            if not settings.admin_api_token:
                self._send_json(
                    HTTPStatus.SERVICE_UNAVAILABLE,
                    {"success": False, "message": "admin api token is not configured"},
                )
                return
            got = str(self.headers.get("X-Admin-Token") or "")
            if got != settings.admin_api_token:
                service._metric_inc("tg_admin_auth_failures_total")
                self._send_json(
                    HTTPStatus.UNAUTHORIZED,
                    {"success": False, "message": "invalid admin token"},
                )
                return
            self._send_json(
                HTTPStatus.OK,
                {
                    "success": True,
                    "result": service.metrics_snapshot(),
                },
            )
            return
        self._send_json(
            HTTPStatus.NOT_FOUND, {"success": False, "message": "not found"}
        )

    def do_POST(self) -> None:
        service = self.service
        settings = self.settings
        if service is None or settings is None:
            self._send_json(
                HTTPStatus.SERVICE_UNAVAILABLE,
                {"success": False, "message": "service unavailable"},
            )
            return

        path = urllib.parse.urlparse(self.path).path

        if path == "/admin/wb-token":
            if not settings.admin_api_token:
                self._send_json(
                    HTTPStatus.SERVICE_UNAVAILABLE,
                    {"success": False, "message": "admin api token is not configured"},
                )
                return
            got_admin_token = str(self.headers.get("X-Admin-Token") or "")
            if got_admin_token != settings.admin_api_token:
                service._metric_inc("tg_admin_auth_failures_total")
                self._send_json(
                    HTTPStatus.UNAUTHORIZED,
                    {"success": False, "message": "invalid admin token"},
                )
                return

            try:
                payload = self._read_json_payload()
            except ValueError as exc:
                self._send_json(
                    HTTPStatus.BAD_REQUEST,
                    {"success": False, "message": str(exc)},
                )
                return

            try:
                result = service.handle_admin_wb_token(payload)
                service._metric_inc("tg_admin_wb_token_success_total")
            except ValueError as exc:
                service._metric_inc("tg_admin_wb_token_failures_total")
                self._send_json(
                    HTTPStatus.BAD_REQUEST,
                    {"success": False, "message": str(exc)},
                )
                return
            except Exception as exc:
                service._metric_inc("tg_admin_wb_token_failures_total")
                self._send_json(
                    HTTPStatus.BAD_GATEWAY,
                    {"success": False, "message": str(exc)},
                )
                return

            self._send_json(
                HTTPStatus.OK,
                {"success": True, "result": result},
            )
            return

        if path != settings.webhook_path:
            self._send_json(
                HTTPStatus.NOT_FOUND, {"success": False, "message": "not found"}
            )
            return

        if settings.webhook_secret:
            got = str(self.headers.get("X-Telegram-Bot-Api-Secret-Token") or "")
            if got != settings.webhook_secret:
                service._metric_inc("tg_webhook_auth_failures_total")
                self._send_json(
                    HTTPStatus.UNAUTHORIZED,
                    {"success": False, "message": "invalid secret"},
                )
                return

        try:
            payload = self._read_json_payload()
        except ValueError as exc:
            self._send_json(
                HTTPStatus.BAD_REQUEST,
                {"success": False, "message": str(exc)},
            )
            return

        try:
            service.handle_update(payload)
        except Exception as exc:
            print(f"[tg-subscriptions] update handling failed: {exc}")
            traceback.print_exc()
            self._send_json(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                {"success": False, "message": "update handling failed"},
            )
            return

        self._send_json(HTTPStatus.OK, {"ok": True})


def run() -> None:
    settings = load_settings()
    service = SubscriptionService(settings)
    service.start()

    WebhookHandler.service = service
    WebhookHandler.settings = settings

    server = ThreadingHTTPServer((settings.host, settings.port), WebhookHandler)
    print(
        f"[tg-subscriptions] listening on http://{settings.host}:{settings.port}"
        f" webhook_path={settings.webhook_path}"
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("[tg-subscriptions] stopping...")
    finally:
        service.stop()
        server.server_close()


if __name__ == "__main__":
    run()
