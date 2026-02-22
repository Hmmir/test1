import datetime
import json
import os
import urllib.error
import urllib.parse
import urllib.request
import uuid


ROOT = r"C:\Users\alien\Desktop\10x"
GS_DUMP_DIR = os.path.join(ROOT, "gs_dump")
XLSX_PATH = os.path.join(ROOT, "sheet_export.xlsx")
CLASPRC_PATH = os.path.expanduser("~/.clasprc.json")
RESULT_PATH = os.path.join(ROOT, "clone_result.json")


def load_clasp_tokens():
    with open(CLASPRC_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data


def save_clasp_tokens(data):
    with open(CLASPRC_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def refresh_access_token(clasp_config):
    token = clasp_config["tokens"]["default"]
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

    token["access_token"] = body["access_token"]
    save_clasp_tokens(clasp_config)
    return body["access_token"]


def api_json(method, url, access_token, payload=None):
    data = None
    headers = {"Authorization": f"Bearer {access_token}"}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json; charset=utf-8"
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req) as resp:
            raw = resp.read()
            return json.loads(raw.decode("utf-8")) if raw else {}
    except urllib.error.HTTPError as e:
        details = e.read().decode("utf-8", "replace")
        raise RuntimeError(f"{method} {url} failed with {e.code}: {details}")


def upload_xlsx_as_google_sheet(access_token):
    if not os.path.exists(XLSX_PATH):
        raise FileNotFoundError(f"Missing source file: {XLSX_PATH}")

    with open(XLSX_PATH, "rb") as f:
        xlsx_bytes = f.read()

    ts = datetime.datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    metadata = {
        "name": f"10x clone {ts}",
        "mimeType": "application/vnd.google-apps.spreadsheet",
    }

    boundary = f"===============_{uuid.uuid4().hex}"
    parts = []
    parts.append(f"--{boundary}\r\n".encode("utf-8"))
    parts.append(b"Content-Type: application/json; charset=UTF-8\r\n\r\n")
    parts.append(json.dumps(metadata).encode("utf-8"))
    parts.append(b"\r\n")
    parts.append(f"--{boundary}\r\n".encode("utf-8"))
    parts.append(
        b"Content-Type: application/vnd.openxmlformats-officedocument.spreadsheetml.sheet\r\n\r\n"
    )
    parts.append(xlsx_bytes)
    parts.append(b"\r\n")
    parts.append(f"--{boundary}--\r\n".encode("utf-8"))
    body = b"".join(parts)

    url = (
        "https://www.googleapis.com/upload/drive/v3/files"
        "?uploadType=multipart&fields=id,name,webViewLink,mimeType"
    )
    req = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": f"multipart/related; boundary={boundary}",
        },
    )
    try:
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        details = e.read().decode("utf-8", "replace")
        raise RuntimeError(f"Drive upload failed with {e.code}: {details}")


def build_script_files_payload():
    files = []
    for name in sorted(os.listdir(GS_DUMP_DIR)):
        if name == ".clasp.json":
            continue
        full = os.path.join(GS_DUMP_DIR, name)
        if not os.path.isfile(full):
            continue

        if name == "appsscript.json":
            file_type = "JSON"
            file_name = "appsscript"
        else:
            stem, ext = os.path.splitext(name)
            if ext.lower() == ".js":
                file_type = "SERVER_JS"
            elif ext.lower() == ".html":
                file_type = "HTML"
            elif ext.lower() == ".json":
                file_type = "JSON"
            else:
                continue
            file_name = stem

        with open(full, "r", encoding="utf-8") as f:
            source = f.read()

        files.append({"name": file_name, "type": file_type, "source": source})

    if not any(f["name"] == "appsscript" and f["type"] == "JSON" for f in files):
        raise RuntimeError("Manifest appsscript.json not found in payload")

    return files


def create_bound_script(access_token, parent_spreadsheet_id):
    title = f"10x clone script {datetime.datetime.utcnow().strftime('%Y%m%d-%H%M%S')}"
    payload = {"title": title, "parentId": parent_spreadsheet_id}
    return api_json(
        "POST", "https://script.googleapis.com/v1/projects", access_token, payload
    )


def update_script_content(access_token, script_id, files_payload):
    url = f"https://script.googleapis.com/v1/projects/{script_id}/content"
    payload = {"files": files_payload}
    return api_json("PUT", url, access_token, payload)


def get_project(access_token, script_id):
    url = f"https://script.googleapis.com/v1/projects/{script_id}"
    return api_json("GET", url, access_token)


def write_result(data):
    with open(RESULT_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def main():
    clasp = load_clasp_tokens()
    access_token = refresh_access_token(clasp)

    sheet = upload_xlsx_as_google_sheet(access_token)
    sheet_id = sheet["id"]

    project = create_bound_script(access_token, sheet_id)
    script_id = project["scriptId"]

    files_payload = build_script_files_payload()
    update_script_content(access_token, script_id, files_payload)

    project_check = get_project(access_token, script_id)
    result = {
        "spreadsheet": sheet,
        "script": {
            "scriptId": script_id,
            "title": project_check.get("title"),
            "parentId": project_check.get("parentId"),
        },
        "filesUploaded": len(files_payload),
    }
    write_result(result)

    print("Spreadsheet ID:", sheet_id)
    print("Spreadsheet URL:", f"https://docs.google.com/spreadsheets/d/{sheet_id}/edit")
    print("Script ID:", script_id)
    print("Script URL:", f"https://script.google.com/home/projects/{script_id}/edit")
    print("Files uploaded:", len(files_payload))
    print("Result saved:", RESULT_PATH)


if __name__ == "__main__":
    main()
