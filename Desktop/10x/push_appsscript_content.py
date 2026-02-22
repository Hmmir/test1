import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from typing import Dict, List, Optional


CLASPRC_PATH = os.path.expanduser("~/.clasprc.json")


def read_json(path: str) -> Dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def refresh_access_token() -> str:
    token = read_json(CLASPRC_PATH)["tokens"]["default"]
    payload = urllib.parse.urlencode(
        {
            "client_id": token["client_id"],
            "client_secret": token["client_secret"],
            "refresh_token": token["refresh_token"],
            "grant_type": "refresh_token",
        }
    ).encode("utf-8")
    req = urllib.request.Request(
        "https://oauth2.googleapis.com/token",
        data=payload,
        method="POST",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read().decode("utf-8"))["access_token"]


def api_request(
    method: str, url: str, token: str, payload: Optional[Dict] = None
) -> Dict:
    data = None
    headers = {"Authorization": f"Bearer {token}"}
    if payload is not None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        headers["Content-Type"] = "application/json; charset=utf-8"
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req) as resp:
            raw = resp.read()
            return json.loads(raw.decode("utf-8")) if raw else {}
    except urllib.error.HTTPError as exc:
        details = exc.read().decode("utf-8", "replace")
        raise RuntimeError(f"{method} {url} failed with {exc.code}: {details}")


def detect_type(name: str) -> Optional[str]:
    if name == "appsscript.json":
        return "JSON"
    ext = os.path.splitext(name)[1].lower()
    if ext in {".js", ".gs"}:
        return "SERVER_JS"
    if ext == ".html":
        return "HTML"
    if ext == ".json":
        return "JSON"
    return None


def build_files(source_dir: str) -> List[Dict]:
    files: List[Dict] = []
    for name in sorted(os.listdir(source_dir)):
        if name == ".clasp.json":
            continue
        full = os.path.join(source_dir, name)
        if not os.path.isfile(full):
            continue
        file_type = detect_type(name)
        if not file_type:
            continue
        with open(full, "r", encoding="utf-8") as f:
            source = f.read()
        file_stem = (
            "appsscript" if name == "appsscript.json" else os.path.splitext(name)[0]
        )
        files.append({"name": file_stem, "type": file_type, "source": source})
    return files


def main() -> None:
    if len(sys.argv) != 3:
        raise SystemExit(
            "Usage: python push_appsscript_content.py <script_id> <source_dir>"
        )
    script_id = sys.argv[1]
    source_dir = sys.argv[2]
    token = refresh_access_token()
    files = build_files(source_dir)
    api_request(
        "PUT",
        f"https://script.googleapis.com/v1/projects/{script_id}/content",
        token,
        {"files": files},
    )
    version = api_request(
        "POST",
        f"https://script.googleapis.com/v1/projects/{script_id}/versions",
        token,
        {"description": "manual content push"},
    )
    print("Pushed files:", len(files))
    print("Version:", version.get("versionNumber"))


if __name__ == "__main__":
    main()
