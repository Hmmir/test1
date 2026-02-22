import json
import os
import urllib.error
import urllib.parse
import urllib.request
from typing import Dict, List, Optional


ROOT = r"C:\Users\alien\Desktop\10x"
CLASPRC_PATH = os.path.expanduser("~/.clasprc.json")
CLONE_RESULT_PATH = os.path.join(ROOT, "clone_result.json")
OUTPUT_PATH = os.path.join(ROOT, "own_stack_result.json")


def read_json(path: str) -> Dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path: str, payload: Dict) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def refresh_access_token() -> str:
    clasp = read_json(CLASPRC_PATH)
    token = clasp["tokens"]["default"]
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
        body = json.loads(resp.read().decode("utf-8"))
    return body["access_token"]


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
            if not raw:
                return {}
            return json.loads(raw.decode("utf-8"))
    except urllib.error.HTTPError as exc:
        details = exc.read().decode("utf-8", "replace")
        raise RuntimeError(f"{method} {url} failed with {exc.code}: {details}")


def detect_file_type(file_name: str) -> Optional[str]:
    if file_name == "appsscript.json":
        return "JSON"
    ext = os.path.splitext(file_name)[1].lower()
    if ext in {".js", ".gs"}:
        return "SERVER_JS"
    if ext == ".html":
        return "HTML"
    if ext == ".json":
        return "JSON"
    return None


def patch_manifest_source(source: str, library_overrides: Dict[str, Dict]) -> str:
    manifest = json.loads(source)
    deps = manifest.setdefault("dependencies", {})
    libs = deps.setdefault("libraries", [])
    if not isinstance(libs, list):
        return source

    for lib in libs:
        if not isinstance(lib, dict):
            continue
        symbol = lib.get("userSymbol")
        if symbol in library_overrides:
            override = library_overrides[symbol]
            lib["libraryId"] = override["scriptId"]
            lib["version"] = str(override["version"])
            lib["developmentMode"] = False

    return json.dumps(manifest, ensure_ascii=False, indent=2)


def build_files_payload(
    source_dir: str, library_overrides: Optional[Dict[str, Dict]] = None
) -> List[Dict]:
    files: List[Dict] = []
    names = sorted(os.listdir(source_dir))
    for name in names:
        if name == ".clasp.json":
            continue
        full = os.path.join(source_dir, name)
        if not os.path.isfile(full):
            continue

        file_type = detect_file_type(name)
        if not file_type:
            continue

        with open(full, "r", encoding="utf-8") as f:
            source = f.read()

        if name == "appsscript.json" and library_overrides:
            source = patch_manifest_source(source, library_overrides)

        file_stem = (
            "appsscript" if name == "appsscript.json" else os.path.splitext(name)[0]
        )
        files.append({"name": file_stem, "type": file_type, "source": source})

    has_manifest = any(f["name"] == "appsscript" and f["type"] == "JSON" for f in files)
    if not has_manifest:
        raise RuntimeError(f"appsscript.json not found in {source_dir}")
    return files


def create_project(token: str, title: str, parent_id: Optional[str] = None) -> Dict:
    payload: Dict[str, str] = {"title": title}
    if parent_id:
        payload["parentId"] = parent_id
    return api_request(
        "POST", "https://script.googleapis.com/v1/projects", token, payload
    )


def update_project_content(token: str, script_id: str, files: List[Dict]) -> Dict:
    url = f"https://script.googleapis.com/v1/projects/{script_id}/content"
    return api_request("PUT", url, token, {"files": files})


def create_version(token: str, script_id: str, description: str) -> Dict:
    url = f"https://script.googleapis.com/v1/projects/{script_id}/versions"
    return api_request("POST", url, token, {"description": description})


def write_manifest_with_overrides(path: str, overrides: Dict[str, Dict]) -> None:
    with open(path, "r", encoding="utf-8") as f:
        source = f.read()
    patched = patch_manifest_source(source, overrides)
    with open(path, "w", encoding="utf-8") as f:
        f.write(patched)


def publish_library(
    token: str,
    title: str,
    source_dir: str,
    overrides: Optional[Dict[str, Dict]] = None,
) -> Dict:
    project = create_project(token, title)
    script_id = project["scriptId"]
    files = build_files_payload(source_dir, overrides)
    update_project_content(token, script_id, files)
    version = create_version(token, script_id, f"bootstrap {title}")
    version_number = int(version["versionNumber"])
    return {
        "scriptId": script_id,
        "version": version_number,
        "title": title,
        "url": f"https://script.google.com/home/projects/{script_id}/edit",
    }


def main() -> None:
    clone = read_json(CLONE_RESULT_PATH)
    target_script_id = clone["script"]["scriptId"]

    token = refresh_access_token()

    libs_meta: Dict[str, Dict] = {}

    libs_meta["MenuParser"] = publish_library(
        token=token,
        title="MenuParser own",
        source_dir=os.path.join(ROOT, "libs", "MenuParser"),
    )
    libs_meta["DB"] = publish_library(
        token=token,
        title="DB own",
        source_dir=os.path.join(ROOT, "libs", "DB"),
    )
    libs_meta["BtlzApi"] = publish_library(
        token=token,
        title="BtlzApi own",
        source_dir=os.path.join(ROOT, "libs", "BtlzApi"),
    )

    common_overrides = {
        "MenuParser": libs_meta["MenuParser"],
        "BtlzApi": libs_meta["BtlzApi"],
    }
    libs_meta["common10x"] = publish_library(
        token=token,
        title="common10x own",
        source_dir=os.path.join(ROOT, "libs", "common10x"),
        overrides=common_overrides,
    )

    utilities_overrides = {
        "common10x": libs_meta["common10x"],
    }
    libs_meta["utilities10X"] = publish_library(
        token=token,
        title="utilities10X own",
        source_dir=os.path.join(ROOT, "libs", "utilities10X"),
        overrides=utilities_overrides,
    )

    main_overrides = {
        "MenuParser": libs_meta["MenuParser"],
        "DB": libs_meta["DB"],
        "BtlzApi": libs_meta["BtlzApi"],
        "common10x": libs_meta["common10x"],
        "utilities10X": libs_meta["utilities10X"],
    }

    write_manifest_with_overrides(
        os.path.join(ROOT, "libs", "common10x", "appsscript.json"),
        common_overrides,
    )
    write_manifest_with_overrides(
        os.path.join(ROOT, "libs", "utilities10X", "appsscript.json"),
        utilities_overrides,
    )
    write_manifest_with_overrides(
        os.path.join(ROOT, "gs_dump", "appsscript.json"),
        main_overrides,
    )

    main_files = build_files_payload(os.path.join(ROOT, "gs_dump"), main_overrides)
    update_project_content(token, target_script_id, main_files)
    main_version = create_version(token, target_script_id, "bootstrap own libraries")

    result = {
        "targetScriptId": target_script_id,
        "targetScriptUrl": f"https://script.google.com/home/projects/{target_script_id}/edit",
        "targetSpreadsheetId": clone["spreadsheet"]["id"],
        "libraries": libs_meta,
        "mainVersion": int(main_version["versionNumber"]),
    }
    write_json(OUTPUT_PATH, result)

    print("Target script:", result["targetScriptId"])
    for symbol, meta in libs_meta.items():
        print(symbol, meta["scriptId"], "v", meta["version"])
    print("Main version:", result["mainVersion"])
    print("Saved:", OUTPUT_PATH)


if __name__ == "__main__":
    main()
