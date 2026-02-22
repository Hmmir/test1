import json
import os
from dataclasses import dataclass
from typing import List


@dataclass(frozen=True)
class PlanConfig:
    code: str
    title: str
    description: str
    amount_int: int
    currency: str
    period_days: int


@dataclass(frozen=True)
class Settings:
    environment: str
    host: str
    port: int
    db_path: str
    bot_token: str
    webhook_path: str
    webhook_secret: str
    webhook_base_url: str
    auto_set_webhook: bool
    command_support_text: str
    admin_api_token: str
    backend_api_base: str
    backend_api_key: str
    backend_bearer_token: str
    request_timeout_seconds: int
    request_retries: int
    webhook_dedupe_retention_days: int
    plans: List[PlanConfig]


def _to_bool(value: str, default: bool = False) -> bool:
    if value is None:
        return default
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return default


def _default_plans() -> List[PlanConfig]:
    return [
        PlanConfig(
            code="pro_monthly",
            title="PRO 30 days",
            description="Access to 10x analytics for 30 days",
            amount_int=1500,
            currency="XTR",
            period_days=30,
        )
    ]


def _load_plans(raw: str) -> List[PlanConfig]:
    if not raw.strip():
        return _default_plans()
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid TG_PLANS_JSON: {exc}") from exc
    if not isinstance(data, list):
        raise ValueError("TG_PLANS_JSON must be a JSON array")

    plans: List[PlanConfig] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        code = str(item.get("code") or "").strip()
        title = str(item.get("title") or "").strip()
        description = str(item.get("description") or "").strip()
        currency = str(item.get("currency") or "XTR").strip().upper()
        period_days = int(item.get("period_days") or 30)
        amount_int = int(item.get("amount_int") or 0)
        if not code or not title or amount_int <= 0:
            continue
        plans.append(
            PlanConfig(
                code=code,
                title=title,
                description=description or title,
                amount_int=amount_int,
                currency=currency,
                period_days=max(period_days, 1),
            )
        )
    if not plans:
        return _default_plans()
    return plans


def load_settings() -> Settings:
    bot_token = str(os.environ.get("TG_BOT_TOKEN") or "").strip()
    if not bot_token:
        raise ValueError("TG_BOT_TOKEN is required")

    environment = str(os.environ.get("TG_ENV", "dev") or "dev").strip().lower()
    webhook_secret = str(os.environ.get("TG_WEBHOOK_SECRET", "")).strip()
    if environment in {"prod", "production"} and not webhook_secret:
        raise ValueError("TG_WEBHOOK_SECRET is required when TG_ENV is production")

    plans = _load_plans(str(os.environ.get("TG_PLANS_JSON") or "").strip())

    return Settings(
        environment=environment,
        host=str(os.environ.get("TG_SVC_HOST", "127.0.0.1")),
        port=int(os.environ.get("TG_SVC_PORT", "8090")),
        db_path=str(
            os.environ.get(
                "TG_SVC_DB_PATH",
                os.path.join(os.path.dirname(__file__), "data", "tg_subscriptions.db"),
            )
        ),
        bot_token=bot_token,
        webhook_path=str(os.environ.get("TG_WEBHOOK_PATH", "/telegram/webhook")),
        webhook_secret=webhook_secret,
        webhook_base_url=str(os.environ.get("TG_WEBHOOK_BASE_URL", "")).strip(),
        auto_set_webhook=_to_bool(
            str(os.environ.get("TG_AUTO_SET_WEBHOOK", "0")), False
        ),
        command_support_text=str(
            os.environ.get(
                "TG_SUPPORT_TEXT",
                "Support: write to @support or your account manager",
            )
        ),
        admin_api_token=str(os.environ.get("TG_ADMIN_API_TOKEN", "")).strip(),
        backend_api_base=str(
            os.environ.get("TG_BACKEND_API_BASE", "http://127.0.0.1:8080/api")
        ),
        backend_api_key=str(os.environ.get("TG_BACKEND_API_KEY", "")).strip(),
        backend_bearer_token=str(os.environ.get("TG_BACKEND_BEARER_TOKEN", "")).strip(),
        request_timeout_seconds=max(
            int(os.environ.get("TG_HTTP_TIMEOUT_SECONDS", "20")), 5
        ),
        request_retries=min(max(int(os.environ.get("TG_HTTP_RETRIES", "2")), 1), 5),
        webhook_dedupe_retention_days=max(
            int(os.environ.get("TG_WEBHOOK_DEDUPE_RETENTION_DAYS", "30")), 1
        ),
        plans=plans,
    )
