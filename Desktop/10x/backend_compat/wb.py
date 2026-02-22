import base64
import json
from typing import Any, Dict, Optional


def decode_wb_token(token: str) -> Dict[str, Any]:
    if not token:
        return {}
    try:
        parts = token.split(".")
        if len(parts) < 2:
            return {}
        payload = parts[1]
        padding = "=" * (-len(payload) % 4)
        payload_bytes = base64.urlsafe_b64decode((payload + padding).encode("utf-8"))
        return json.loads(payload_bytes.decode("utf-8"))
    except Exception:
        return {}


def token_sid(token: str) -> Optional[str]:
    payload = decode_wb_token(token)
    sid = payload.get("sid")
    return str(sid) if sid else None
