import json
import time
import urllib.error
import urllib.request
from typing import Any, Dict


class OnboardingBridge:
    def __init__(
        self,
        base_url: str,
        api_key: str = "",
        bearer_token: str = "",
        timeout_seconds: int = 20,
        request_retries: int = 2,
    ):
        self.base_url = str(base_url or "").rstrip("/")
        self.api_key = str(api_key or "").strip()
        self.bearer_token = str(bearer_token or "").strip()
        self.timeout_seconds = timeout_seconds
        self.request_retries = max(int(request_retries or 1), 1)

    def enabled(self) -> bool:
        return bool(self.base_url)

    def _post(self, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        if not self.base_url:
            raise RuntimeError("onboarding bridge is disabled")

        url = f"{self.base_url}{path}"
        headers = {"Content-Type": "application/json"}
        if self.bearer_token:
            headers["Authorization"] = f"Bearer {self.bearer_token}"
        elif self.api_key:
            headers["X-Api-Key"] = self.api_key

        req = urllib.request.Request(
            url,
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            headers=headers,
            method="POST",
        )
        raw = ""
        last_error: Exception | None = None
        for attempt in range(self.request_retries):
            try:
                with urllib.request.urlopen(req, timeout=self.timeout_seconds) as resp:
                    raw = resp.read().decode("utf-8")
                last_error = None
                break
            except urllib.error.HTTPError as exc:
                body = exc.read().decode("utf-8", errors="ignore")
                should_retry = (
                    500 <= int(exc.code) < 600 and attempt + 1 < self.request_retries
                )
                if should_retry:
                    time.sleep(0.2 * (2**attempt))
                    continue
                raise RuntimeError(f"{path} failed: {exc.code} {body}") from exc
            except Exception as exc:
                last_error = exc
                if attempt + 1 >= self.request_retries:
                    break
                time.sleep(0.2 * (2**attempt))

        if last_error is not None:
            raise RuntimeError(f"{path} failed: {last_error}") from last_error

        try:
            data = json.loads(raw) if raw else {}
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"{path} returned invalid JSON: {raw[:200]}") from exc
        if not isinstance(data, dict):
            raise RuntimeError(f"{path} returned non-object response")
        return data

    def add_wb_token(
        self, spreadsheet_id: str, email: str, wb_token: str
    ) -> Dict[str, Any]:
        return self._post(
            "/admin/wb/tokens/add",
            {
                "spreadsheet_id": spreadsheet_id,
                "owner_email": email,
                "token": wb_token,
            },
        )

    def provision_client(self, email: str, spreadsheet_id: str) -> Dict[str, Any]:
        out: Dict[str, Any] = {}

        out["register"] = self._post(
            "/admin/spreadsheets/register",
            {
                "spreadsheet_id": spreadsheet_id,
                "owner_email": email,
            },
        )

        token_get = self._post(
            "/ss/wb/token/get",
            {
                "spreadsheet_id": spreadsheet_id,
            },
        )
        out["token_get"] = token_get
        result_rows = token_get.get("result")
        if not isinstance(result_rows, list) or not result_rows:
            raise RuntimeError(
                "no active WB token for spreadsheet. Add token via secure admin flow and retry"
            )

        out["datasets_update"] = self._post(
            "/ss/datasets/update",
            {
                "ssId": spreadsheet_id,
            },
        )
        return out
