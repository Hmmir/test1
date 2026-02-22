import json
import time
import urllib.error
import urllib.request
from typing import Any, Dict, Optional


class TelegramApiError(RuntimeError):
    pass


class TelegramApi:
    def __init__(
        self,
        bot_token: str,
        timeout_seconds: int = 20,
        request_retries: int = 2,
    ):
        self.bot_token = bot_token
        self.timeout_seconds = timeout_seconds
        self.request_retries = max(int(request_retries or 1), 1)
        self.base_url = f"https://api.telegram.org/bot{bot_token}"

    def _call(self, method: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        req = urllib.request.Request(
            f"{self.base_url}/{method}",
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        body = ""
        last_error: Exception | None = None
        for attempt in range(self.request_retries):
            try:
                with urllib.request.urlopen(req, timeout=self.timeout_seconds) as resp:
                    body = resp.read().decode("utf-8")
                last_error = None
                break
            except urllib.error.HTTPError as exc:
                err_body = exc.read().decode("utf-8", errors="ignore")
                should_retry = (
                    500 <= int(exc.code) < 600 and attempt + 1 < self.request_retries
                )
                if should_retry:
                    time.sleep(0.2 * (2**attempt))
                    continue
                raise TelegramApiError(
                    f"{method} failed: {exc.code} {err_body}"
                ) from exc
            except Exception as exc:
                last_error = exc
                if attempt + 1 >= self.request_retries:
                    break
                time.sleep(0.2 * (2**attempt))

        if last_error is not None:
            raise TelegramApiError(f"{method} failed: {last_error}") from last_error

        try:
            result = json.loads(body)
        except json.JSONDecodeError as exc:
            raise TelegramApiError(
                f"{method} invalid JSON response: {body[:200]}"
            ) from exc

        if not isinstance(result, dict) or not result.get("ok"):
            raise TelegramApiError(f"{method} returned error: {result}")
        return result

    def set_webhook(self, url: str, secret_token: str = "") -> Dict[str, Any]:
        payload: Dict[str, Any] = {"url": url}
        if secret_token:
            payload["secret_token"] = secret_token
        return self._call("setWebhook", payload)

    def send_message(
        self,
        chat_id: int,
        text: str,
        parse_mode: Optional[str] = None,
        disable_web_page_preview: bool = True,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "chat_id": chat_id,
            "text": text,
            "disable_web_page_preview": disable_web_page_preview,
        }
        if parse_mode:
            payload["parse_mode"] = parse_mode
        return self._call("sendMessage", payload)

    def send_invoice(
        self,
        chat_id: int,
        title: str,
        description: str,
        payload_value: str,
        currency: str,
        amount_int: int,
        period_days: int,
    ) -> Dict[str, Any]:
        prices = [{"label": title, "amount": int(amount_int)}]
        payload: Dict[str, Any] = {
            "chat_id": chat_id,
            "title": title,
            "description": description,
            "payload": payload_value,
            "currency": currency,
            "prices": prices,
        }

        # For Telegram Stars (XTR) provider_token must be omitted.
        if currency.upper() == "XTR" and period_days == 30:
            payload["subscription_period"] = 2592000

        return self._call("sendInvoice", payload)

    def answer_pre_checkout_query(
        self,
        pre_checkout_query_id: str,
        ok: bool,
        error_message: str = "",
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "pre_checkout_query_id": pre_checkout_query_id,
            "ok": bool(ok),
        }
        if not ok and error_message:
            payload["error_message"] = error_message
        return self._call("answerPreCheckoutQuery", payload)
