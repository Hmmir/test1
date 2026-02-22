import os
import gc
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


SERVICE_DIR = Path(__file__).resolve().parents[1]
if str(SERVICE_DIR) not in sys.path:
    sys.path.insert(0, str(SERVICE_DIR))

import config  # noqa: E402
import main  # noqa: E402
import storage  # noqa: E402


def _build_settings(db_path: str) -> config.Settings:
    return config.Settings(
        environment="dev",
        host="127.0.0.1",
        port=8090,
        db_path=db_path,
        bot_token="test-token",
        webhook_path="/telegram/webhook",
        webhook_secret="secret",
        webhook_base_url="",
        auto_set_webhook=False,
        command_support_text="support",
        admin_api_token="admin",
        backend_api_base="http://127.0.0.1:8080/api",
        backend_api_key="",
        backend_bearer_token="",
        request_timeout_seconds=5,
        request_retries=1,
        webhook_dedupe_retention_days=30,
        plans=[
            config.PlanConfig(
                code="pro_monthly",
                title="PRO",
                description="desc",
                amount_int=1500,
                currency="XTR",
                period_days=30,
            )
        ],
    )


class OnboardingGuardrailTests(unittest.TestCase):
    def test_onboard_requires_active_subscription(self):
        with tempfile.TemporaryDirectory() as tmp:
            settings = _build_settings(str(Path(tmp) / "tg.db"))
            service = main.SubscriptionService(settings)

            service.onboarding = mock.Mock()
            service.onboarding.enabled.return_value = True
            service.onboarding.provision_client.return_value = {"success": True}

            service.storage.get_active_subscription = mock.Mock(return_value=None)
            service.storage.upsert_onboarding_profile = mock.Mock()

            sent_messages = []
            service._safe_send_message = lambda chat_id, text: sent_messages.append(
                text
            )

            service._cmd_onboard(1, 1001, "owner@example.com sheet-1")

            self.assertTrue(sent_messages)
            self.assertIn("No active subscription", sent_messages[0])
            service.storage.upsert_onboarding_profile.assert_not_called()
            service.onboarding.provision_client.assert_not_called()
            self.assertEqual(
                service.metrics_snapshot().get("tg_onboarding_unpaid_rejections_total"),
                1,
            )

    def test_onboard_provisions_when_subscription_is_active(self):
        with tempfile.TemporaryDirectory() as tmp:
            settings = _build_settings(str(Path(tmp) / "tg.db"))
            service = main.SubscriptionService(settings)

            service.onboarding = mock.Mock()
            service.onboarding.enabled.return_value = True
            service.onboarding.provision_client.return_value = {"success": True}

            active = storage.ActiveSubscription(
                plan_code="pro_monthly",
                starts_at="2026-01-01T00:00:00Z",
                ends_at="2026-02-01T00:00:00Z",
                status="active",
            )
            service.storage.get_active_subscription = mock.Mock(return_value=active)
            service.storage.upsert_onboarding_profile = mock.Mock()
            service.storage.set_onboarding_result = mock.Mock()

            sent_messages = []
            service._safe_send_message = lambda chat_id, text: sent_messages.append(
                text
            )

            service._cmd_onboard(1, 1001, "owner@example.com sheet-1")

            service.storage.upsert_onboarding_profile.assert_called_once()
            service.onboarding.provision_client.assert_called_once_with(
                "owner@example.com", "sheet-1"
            )
            service.storage.set_onboarding_result.assert_called_once_with(1001, ok=True)
            self.assertTrue(
                any("Onboarding completed" in text for text in sent_messages)
            )


class WebhookIdempotencyTests(unittest.TestCase):
    def test_storage_register_webhook_update_is_idempotent(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "tg.db")
            st = storage.Storage(db_path)

            self.assertTrue(st.register_webhook_update(2001))
            self.assertFalse(st.register_webhook_update(2001))
            self.assertTrue(st.register_webhook_update(0))

    def test_service_deduplicates_by_update_id(self):
        with tempfile.TemporaryDirectory() as tmp:
            settings = _build_settings(str(Path(tmp) / "tg.db"))
            service = main.SubscriptionService(settings)

            handled = []
            service._handle_pre_checkout_query = lambda query: handled.append(query)

            update = {
                "update_id": 404,
                "pre_checkout_query": {
                    "id": "pcq-1",
                    "invoice_payload": "payload-1",
                },
            }
            service.handle_update(update)
            service.handle_update(update)

            self.assertEqual(len(handled), 1)
            metrics = service.metrics_snapshot()
            self.assertEqual(metrics.get("tg_updates_total"), 1)
            self.assertEqual(metrics.get("tg_webhook_duplicates_total"), 1)

    def test_prune_processed_updates_removes_old_rows(self):
        fd, db_path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        try:
            st = storage.Storage(db_path)
            st.register_webhook_update(7001)
            with st._connect() as conn:  # noqa: SLF001 - test-only setup
                conn.execute(
                    "UPDATE processed_updates SET processed_at = '2000-01-01T00:00:00Z' WHERE update_id = ?",
                    (7001,),
                )
            removed = st.prune_processed_updates(retention_days=1)
            self.assertGreaterEqual(removed, 1)
        finally:
            gc.collect()


class PaymentActivationGuardrailTests(unittest.TestCase):
    def test_activation_rejects_unknown_payload(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "tg.db")
            st = storage.Storage(db_path)
            with self.assertRaises(ValueError):
                st.activate_subscription_from_payment(
                    telegram_user_id=11,
                    payload="missing-payload",
                    telegram_payment_charge_id="tg-1",
                    provider_payment_charge_id="prov-1",
                    amount_int=1500,
                    currency="XTR",
                    raw_json="{}",
                )

    def test_activation_rejects_user_mismatch(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = str(Path(tmp) / "tg.db")
            st = storage.Storage(db_path)
            st.create_payment_intent(
                telegram_user_id=101,
                plan_code="pro_monthly",
                payload="payload-1",
                amount_int=1500,
                currency="XTR",
                raw_json="{}",
            )

            with self.assertRaises(ValueError):
                st.activate_subscription_from_payment(
                    telegram_user_id=202,
                    payload="payload-1",
                    telegram_payment_charge_id="tg-2",
                    provider_payment_charge_id="prov-2",
                    amount_int=1500,
                    currency="XTR",
                    raw_json="{}",
                )


class ProductionWebhookSecretTests(unittest.TestCase):
    def test_webhook_secret_required_in_production(self):
        with mock.patch.dict(
            os.environ,
            {
                "TG_BOT_TOKEN": "token",
                "TG_ENV": "production",
                "TG_WEBHOOK_SECRET": "",
            },
            clear=True,
        ):
            with self.assertRaises(ValueError):
                config.load_settings()

    def test_webhook_secret_can_be_empty_in_dev(self):
        with mock.patch.dict(
            os.environ,
            {
                "TG_BOT_TOKEN": "token",
                "TG_ENV": "dev",
                "TG_WEBHOOK_SECRET": "",
            },
            clear=True,
        ):
            settings = config.load_settings()
        self.assertEqual(settings.environment, "dev")
        self.assertEqual(settings.webhook_secret, "")


if __name__ == "__main__":
    unittest.main()
