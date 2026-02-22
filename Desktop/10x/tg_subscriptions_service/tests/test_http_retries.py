import io
import sys
import unittest
import urllib.error
from email.message import Message
from pathlib import Path
from unittest import mock


SERVICE_DIR = Path(__file__).resolve().parents[1]
if str(SERVICE_DIR) not in sys.path:
    sys.path.insert(0, str(SERVICE_DIR))

import onboarding_bridge  # noqa: E402
import telegram_api  # noqa: E402


class _FakeResponse:
    def __init__(self, body: str):
        self._body = body.encode("utf-8")

    def read(self) -> bytes:
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        del exc_type, exc, tb
        return False


def _http_error(code: int, body: str = "{}") -> urllib.error.HTTPError:
    return urllib.error.HTTPError(
        url="http://example.local",
        code=code,
        msg="error",
        hdrs=Message(),
        fp=io.BytesIO(body.encode("utf-8")),
    )


class OnboardingBridgeRetryTests(unittest.TestCase):
    def test_retries_on_transient_network_error(self):
        bridge = onboarding_bridge.OnboardingBridge(
            base_url="http://127.0.0.1:8080/api",
            request_retries=2,
        )
        with (
            mock.patch(
                "onboarding_bridge.urllib.request.urlopen",
                side_effect=[
                    urllib.error.URLError("boom"),
                    _FakeResponse('{"success": true}'),
                ],
            ) as mocked,
            mock.patch("onboarding_bridge.time.sleep", return_value=None),
        ):
            result = bridge._post(
                "/admin/spreadsheets/register", {"spreadsheet_id": "ss-1"}
            )

        self.assertTrue(result.get("success"))
        self.assertEqual(mocked.call_count, 2)

    def test_does_not_retry_on_http_400(self):
        bridge = onboarding_bridge.OnboardingBridge(
            base_url="http://127.0.0.1:8080/api",
            request_retries=3,
        )
        with (
            mock.patch(
                "onboarding_bridge.urllib.request.urlopen",
                side_effect=[_http_error(400, "bad request")],
            ) as mocked,
            mock.patch("onboarding_bridge.time.sleep", return_value=None),
        ):
            with self.assertRaises(RuntimeError):
                bridge._post("/admin/spreadsheets/register", {"spreadsheet_id": "ss-1"})

        self.assertEqual(mocked.call_count, 1)

    def test_uses_bearer_auth_when_bearer_token_provided(self):
        bridge = onboarding_bridge.OnboardingBridge(
            base_url="http://127.0.0.1:8080/api",
            api_key="legacy-key",
            bearer_token="bearer-token",
            request_retries=1,
        )
        captured = {"request": None}

        def _fake_urlopen(request, timeout=0):
            del timeout
            captured["request"] = request
            return _FakeResponse('{"success": true}')

        with mock.patch(
            "onboarding_bridge.urllib.request.urlopen", side_effect=_fake_urlopen
        ):
            result = bridge._post(
                "/admin/spreadsheets/register", {"spreadsheet_id": "ss-1"}
            )

        self.assertTrue(result.get("success"))
        req = captured["request"]
        self.assertIsNotNone(req)
        if req is None:
            self.fail("request must be captured")
        headers = getattr(req, "headers", {})
        self.assertEqual(headers.get("Authorization"), "Bearer bearer-token")
        self.assertIsNone(headers.get("X-api-key"))


class TelegramApiRetryTests(unittest.TestCase):
    def test_retries_on_http_5xx(self):
        api = telegram_api.TelegramApi(
            bot_token="test-token",
            request_retries=2,
        )
        with (
            mock.patch(
                "telegram_api.urllib.request.urlopen",
                side_effect=[
                    _http_error(502, "bad gateway"),
                    _FakeResponse('{"ok": true, "result": {}}'),
                ],
            ) as mocked,
            mock.patch("telegram_api.time.sleep", return_value=None),
        ):
            result = api._call("sendMessage", {"chat_id": 1, "text": "hi"})

        self.assertTrue(result.get("ok"))
        self.assertEqual(mocked.call_count, 2)

    def test_does_not_retry_on_http_400(self):
        api = telegram_api.TelegramApi(
            bot_token="test-token",
            request_retries=3,
        )
        with (
            mock.patch(
                "telegram_api.urllib.request.urlopen",
                side_effect=[_http_error(400, "bad request")],
            ) as mocked,
            mock.patch("telegram_api.time.sleep", return_value=None),
        ):
            with self.assertRaises(telegram_api.TelegramApiError):
                api._call("sendMessage", {"chat_id": 1, "text": "hi"})

        self.assertEqual(mocked.call_count, 1)


if __name__ == "__main__":
    unittest.main()
