import os
import threading
import time
from datetime import datetime
from typing import List, Optional

from collector import run_collection
from storage import Storage


def _to_bool(value: str, default: bool) -> bool:
    text = str(value or "").strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return default


def _parse_nm_ids(raw: str) -> Optional[List[int]]:
    text = str(raw or "").strip()
    if not text:
        return None
    out: List[int] = []
    for part in text.split(","):
        item = part.strip()
        if not item:
            continue
        try:
            nm_id = int(float(item))
        except (TypeError, ValueError):
            continue
        if nm_id > 0:
            out.append(nm_id)
    return out or None


def main() -> None:
    db_path = str(
        os.environ.get(
            "BTLZ_DB_PATH",
            os.path.join(os.path.dirname(__file__), "data", "btlz.db"),
        )
    )
    spreadsheet_id = str(os.environ.get("BTLZ_COLLECTOR_SHEET_ID") or "").strip()
    if not spreadsheet_id:
        raise RuntimeError("BTLZ_COLLECTOR_SHEET_ID is required")

    interval_minutes = max(int(os.environ.get("BTLZ_COLLECTOR_INTERVAL_MIN", "360")), 5)
    with_positions = _to_bool(
        str(os.environ.get("BTLZ_COLLECTOR_WITH_POSITIONS", "0")), False
    )
    with_funnel = _to_bool(str(os.environ.get("BTLZ_COLLECTOR_WITH_FUNNEL", "1")), True)
    run_once = _to_bool(str(os.environ.get("BTLZ_COLLECTOR_ONCE", "0")), False)
    nm_ids = _parse_nm_ids(str(os.environ.get("BTLZ_COLLECTOR_NM_IDS", "") or ""))
    lock_ttl_seconds = max(
        int(os.environ.get("BTLZ_COLLECTOR_LOCK_TTL_SECONDS", "1800") or 1800),
        60,
    )

    storage = Storage(db_path)
    lock_owner = f"daemon:{os.getpid()}"

    while True:
        day = datetime.utcnow().strftime("%Y-%m-%d")
        acquired, lock_state = storage.acquire_collector_lock(
            owner=lock_owner,
            ttl_seconds=lock_ttl_seconds,
        )
        if not acquired:
            print(
                f"[collector-daemon] skipped {day}: collector lock is busy {lock_state}"
            )
            if run_once:
                break
            time.sleep(interval_minutes * 60)
            continue

        heartbeat_stop = threading.Event()
        heartbeat_lost = {"lost": False}

        def _heartbeat() -> None:
            interval = max(15, min(120, int(lock_ttl_seconds // 3) or 15))
            while not heartbeat_stop.wait(interval):
                ok = storage.refresh_collector_lock(
                    owner=lock_owner,
                    ttl_seconds=lock_ttl_seconds,
                )
                if not ok:
                    heartbeat_lost["lost"] = True
                    break

        heartbeat_thread = threading.Thread(
            target=_heartbeat,
            name="collector-daemon-heartbeat",
            daemon=True,
        )
        heartbeat_thread.start()

        try:
            summary = run_collection(
                storage=storage,
                spreadsheet_id=spreadsheet_id,
                date_from=day,
                date_to=day,
                nm_ids=nm_ids,
                with_positions=with_positions,
                with_funnel=with_funnel,
            )
            print(f"[collector-daemon] ok {day} {summary}")
        except Exception as exc:
            print(f"[collector-daemon] failed {day}: {exc}")
        finally:
            heartbeat_stop.set()
            heartbeat_thread.join(timeout=2)
            try:
                storage.release_collector_lock(lock_owner)
            except Exception as exc:
                print(f"[collector-daemon] failed to release lock: {exc}")

        if heartbeat_lost.get("lost"):
            print(
                "[collector-daemon] lock heartbeat failed, skipping immediate rerun cycle"
            )

        if run_once:
            break

        time.sleep(interval_minutes * 60)


if __name__ == "__main__":
    main()
