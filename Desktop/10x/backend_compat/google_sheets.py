import json
import os
import urllib.parse
from dataclasses import dataclass
from typing import Any, Dict, List, Optional


def _load_service_account_info() -> Optional[Dict[str, Any]]:
    raw = str(os.environ.get("BTLZ_GOOGLE_SA_JSON", "") or "").strip()
    if raw:
        try:
            payload = json.loads(raw)
            return payload if isinstance(payload, dict) else None
        except json.JSONDecodeError:
            return None
    path = str(os.environ.get("BTLZ_GOOGLE_SA_FILE", "") or "").strip()
    if not path:
        return None
    try:
        with open(path, "r", encoding="utf-8") as fh:
            payload = json.load(fh)
        return payload if isinstance(payload, dict) else None
    except Exception:
        return None


def _safe_sheet_title(title: str) -> str:
    # Sheet API ranges must single-quote titles with special characters.
    text = str(title or "")
    return "'" + text.replace("'", "''") + "'"


@dataclass
class GoogleSheetsClient:
    spreadsheet_scope: str = "https://www.googleapis.com/auth/spreadsheets"

    def __post_init__(self) -> None:
        # Late imports to keep module importable even without google-auth deps.
        from google.auth.transport.requests import AuthorizedSession
        from google.oauth2.service_account import Credentials

        info = _load_service_account_info()
        if not info:
            raise RuntimeError(
                "Google service account is not configured. "
                "Set BTLZ_GOOGLE_SA_JSON or BTLZ_GOOGLE_SA_FILE."
            )
        creds = Credentials.from_service_account_info(
            info, scopes=[self.spreadsheet_scope]
        )
        self._session = AuthorizedSession(creds)
        # Many environments set a broken global HTTP(S)_PROXY. Default to ignoring env proxies.
        use_env_proxy = str(
            os.environ.get("BTLZ_USE_ENV_PROXY", "0") or ""
        ).strip().lower() in {
            "1",
            "true",
            "yes",
        }
        if not use_env_proxy:
            try:
                self._session.trust_env = False
            except Exception:
                pass

    @classmethod
    def from_env(cls) -> Optional["GoogleSheetsClient"]:
        if _load_service_account_info() is None:
            return None
        try:
            return cls()
        except Exception:
            return None

    def _get(self, url: str, timeout: int = 60) -> Any:
        resp = self._session.get(url, timeout=timeout)
        if resp.status_code >= 400:
            raise RuntimeError(f"Google Sheets API {resp.status_code}: {resp.text}")
        if not resp.text:
            return {}
        return resp.json()

    def _post(self, url: str, payload: Dict[str, Any], timeout: int = 60) -> Any:
        resp = self._session.post(url, json=payload, timeout=timeout)
        if resp.status_code >= 400:
            raise RuntimeError(f"Google Sheets API {resp.status_code}: {resp.text}")
        if not resp.text:
            return {}
        return resp.json()

    def _clear(self, url: str, timeout: int = 60) -> Any:
        resp = self._session.post(url, json={}, timeout=timeout)
        if resp.status_code >= 400:
            raise RuntimeError(f"Google Sheets API {resp.status_code}: {resp.text}")
        if not resp.text:
            return {}
        return resp.json()

    def get_spreadsheet(
        self, spreadsheet_id: str, fields: str = "sheets.properties"
    ) -> Dict[str, Any]:
        url = (
            f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}"
            f"?fields={urllib.parse.quote(fields, safe=',.*')}"
        )
        data = self._get(url)
        return data if isinstance(data, dict) else {}

    def resolve_sheet_id(self, spreadsheet_id: str, sheet_name: str) -> Optional[int]:
        data = self.get_spreadsheet(spreadsheet_id, fields="sheets.properties")
        sheets = data.get("sheets") if isinstance(data, dict) else None
        if not isinstance(sheets, list):
            return None
        for sheet in sheets:
            if not isinstance(sheet, dict):
                continue
            raw_props = sheet.get("properties")
            if not isinstance(raw_props, dict):
                continue
            props: Dict[str, Any] = raw_props
            if str(props.get("title") or "") == str(sheet_name or ""):
                sheet_id_raw = props.get("sheetId")
                if sheet_id_raw is None:
                    return None
                try:
                    return int(sheet_id_raw)
                except (TypeError, ValueError):
                    return None
        return None

    def dedupe_conditional_format_rules(
        self, spreadsheet_id: str, sheet_name: str
    ) -> Dict[str, Any]:
        fields = "sheets.properties,sheets.conditionalFormats"
        data = self.get_spreadsheet(spreadsheet_id, fields=fields)
        sheets = data.get("sheets") if isinstance(data, dict) else None
        if not isinstance(sheets, list):
            raise RuntimeError("Failed to fetch spreadsheet sheets list")

        requests: List[Dict[str, Any]] = []
        deleted_by_sheet: Dict[str, int] = {}

        for sheet in sheets:
            if not isinstance(sheet, dict):
                continue
            raw_props = sheet.get("properties")
            if not isinstance(raw_props, dict):
                continue
            props: Dict[str, Any] = raw_props
            title = str(props.get("title") or "")
            if sheet_name and title != sheet_name:
                continue

            sheet_id_raw = props.get("sheetId")
            if sheet_id_raw is None:
                continue
            try:
                sheet_id = int(sheet_id_raw)
            except (TypeError, ValueError):
                continue

            rules = sheet.get("conditionalFormats")
            if not isinstance(rules, list) or not rules:
                continue

            seen = set()
            dup_indices: List[int] = []
            for idx, rule in enumerate(rules):
                if not isinstance(rule, dict):
                    continue
                fingerprint = json.dumps(rule, sort_keys=True, ensure_ascii=True)
                if fingerprint in seen:
                    dup_indices.append(idx)
                else:
                    seen.add(fingerprint)

            # Delete indices in reverse order, API expects current index.
            dup_indices.sort(reverse=True)
            for idx in dup_indices:
                requests.append(
                    {
                        "deleteConditionalFormatRule": {
                            "sheetId": sheet_id,
                            "index": int(idx),
                        }
                    }
                )

            if dup_indices:
                deleted_by_sheet[title] = len(dup_indices)

        if not requests:
            return {
                "success": True,
                "deleted": 0,
                "sheets": deleted_by_sheet,
                "message": "no duplicates found",
            }

        url = f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}:batchUpdate"
        self._post(url, {"requests": requests})
        return {
            "success": True,
            "deleted": sum(deleted_by_sheet.values()),
            "sheets": deleted_by_sheet,
            "message": "conditional format duplicates removed",
        }

    def values_get(self, spreadsheet_id: str, a1_range: str) -> List[List[Any]]:
        encoded = urllib.parse.quote(a1_range, safe="!'():,")
        url = f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}/values/{encoded}"
        data = self._get(url)
        values = data.get("values") if isinstance(data, dict) else None
        return values if isinstance(values, list) else []

    def values_clear(self, spreadsheet_id: str, a1_range: str) -> None:
        encoded = urllib.parse.quote(a1_range, safe="!'():,")
        url = f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}/values/{encoded}:clear"
        self._clear(url)

    def values_update(
        self,
        spreadsheet_id: str,
        a1_range: str,
        values: List[List[Any]],
        value_input_option: str = "USER_ENTERED",
    ) -> None:
        encoded = urllib.parse.quote(a1_range, safe="!'():,")
        url = f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}/values/{encoded}?valueInputOption={urllib.parse.quote(value_input_option)}"
        self._session.put(
            url,
            json={"values": values},
            timeout=60,
        ).raise_for_status()

    def replace_sheet_table(
        self,
        spreadsheet_id: str,
        sheet_name: str,
        headers: List[str],
        rows: List[List[Any]],
        header_row: int = 1,
        data_row_first: int = 3,
    ) -> Dict[str, Any]:
        sheet_id = self.resolve_sheet_id(spreadsheet_id, sheet_name)
        if sheet_id is None:
            raise RuntimeError(f"sheet not found: {sheet_name}")

        safe_title = _safe_sheet_title(sheet_name)
        # Ensure header row exists (only fill if empty).
        header_range = f"{safe_title}!A{header_row}:ZZ{header_row}"
        current = self.values_get(spreadsheet_id, header_range)
        current_row = current[0] if current and isinstance(current[0], list) else []
        if not any(str(x or "").strip() for x in current_row):
            self.values_update(
                spreadsheet_id,
                f"{safe_title}!A{header_row}:{_col_letter(len(headers))}{header_row}",
                [headers],
            )

        # Clear old data from data_row_first downward for the existing header width.
        width = max(len(headers), 1)
        clear_range = f"{safe_title}!A{data_row_first}:{_col_letter(width)}"
        self.values_clear(spreadsheet_id, clear_range)

        if not rows:
            return {"success": True, "written": 0, "sheet": sheet_name}

        target_range = f"{safe_title}!A{data_row_first}:{_col_letter(width)}{data_row_first + len(rows) - 1}"
        self.values_update(spreadsheet_id, target_range, rows)
        return {"success": True, "written": len(rows), "sheet": sheet_name}


def _col_letter(col_num: int) -> str:
    if col_num < 1:
        return "A"
    letters = ""
    n = int(col_num)
    while n > 0:
        n, rem = divmod(n - 1, 26)
        letters = chr(65 + rem) + letters
    return letters
