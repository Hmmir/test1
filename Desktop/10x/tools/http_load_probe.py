import argparse
import concurrent.futures
import json
import math
import sys
import time
import urllib.error
import urllib.request
from typing import Dict, List, Tuple


def _percentile(values: List[float], p: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = max(0, min(len(ordered) - 1, math.ceil((p / 100.0) * len(ordered)) - 1))
    return float(ordered[idx])


def _parse_json_map(raw: str) -> Dict[str, str]:
    text = str(raw or "").strip()
    if not text:
        return {}
    data = json.loads(text)
    if not isinstance(data, dict):
        raise ValueError("JSON map is required")
    out: Dict[str, str] = {}
    for key, value in data.items():
        out[str(key)] = str(value)
    return out


def _single_request(
    url: str,
    method: str,
    headers: Dict[str, str],
    body: bytes,
    timeout: float,
) -> Tuple[int, float, bool, bool]:
    req = urllib.request.Request(
        url=url,
        data=body if method in {"POST", "PUT", "PATCH", "DELETE"} else None,
        method=method,
    )
    for key, value in headers.items():
        req.add_header(key, value)

    started = time.perf_counter()
    status = 0
    locked = False
    ok = False
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            status = int(getattr(resp, "status", 0) or 0)
            payload = resp.read().decode("utf-8", errors="ignore")
            locked = "database is locked" in payload.lower()
            ok = 200 <= status < 300
    except urllib.error.HTTPError as exc:
        status = int(exc.code or 0)
        payload = exc.read().decode("utf-8", errors="ignore")
        locked = "database is locked" in payload.lower()
    except Exception as exc:
        payload = str(exc)
        locked = "database is locked" in payload.lower()

    elapsed_ms = (time.perf_counter() - started) * 1000.0
    return status, elapsed_ms, ok, locked


def main() -> int:
    parser = argparse.ArgumentParser(description="Simple concurrent HTTP load probe")
    parser.add_argument("--url", action="append", required=True)
    parser.add_argument("--requests", type=int, default=200)
    parser.add_argument("--concurrency", type=int, default=20)
    parser.add_argument("--method", default="GET")
    parser.add_argument("--timeout-seconds", type=float, default=10.0)
    parser.add_argument("--headers-json", default="{}")
    parser.add_argument("--body-json", default="")
    parser.add_argument("--max-error-rate", type=float, default=0.02)
    parser.add_argument("--max-p95-ms", type=float, default=1000.0)
    args = parser.parse_args()

    method = str(args.method or "GET").strip().upper()
    if method not in {"GET", "POST", "PUT", "PATCH", "DELETE"}:
        raise ValueError("Unsupported HTTP method")

    headers = _parse_json_map(args.headers_json)
    body = b""
    if args.body_json:
        body = str(args.body_json).encode("utf-8")
        headers.setdefault("Content-Type", "application/json")

    total = max(int(args.requests or 1), 1)
    workers = max(int(args.concurrency or 1), 1)
    urls = [str(item).strip() for item in args.url if str(item).strip()]
    if not urls:
        raise ValueError("At least one --url is required")

    latencies: List[float] = []
    status_counts: Dict[str, int] = {}
    ok_count = 0
    locked_errors = 0

    def _run(i: int) -> Tuple[int, float, bool, bool]:
        url = urls[i % len(urls)]
        return _single_request(
            url=url,
            method=method,
            headers=headers,
            body=body,
            timeout=float(args.timeout_seconds),
        )

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(_run, i) for i in range(total)]
        for fut in concurrent.futures.as_completed(futures):
            status, elapsed_ms, ok, locked = fut.result()
            latencies.append(elapsed_ms)
            status_key = str(status)
            status_counts[status_key] = int(status_counts.get(status_key, 0)) + 1
            if ok:
                ok_count += 1
            if locked:
                locked_errors += 1

    error_count = total - ok_count
    error_rate = float(error_count) / float(total)
    p95_ms = _percentile(latencies, 95.0)
    result = {
        "total": total,
        "ok": ok_count,
        "errors": error_count,
        "error_rate": round(error_rate, 6),
        "p95_ms": round(p95_ms, 3),
        "database_locked_errors": locked_errors,
        "status_counts": status_counts,
        "thresholds": {
            "max_error_rate": float(args.max_error_rate),
            "max_p95_ms": float(args.max_p95_ms),
        },
        "pass": (
            error_rate <= float(args.max_error_rate)
            and p95_ms <= float(args.max_p95_ms)
            and locked_errors == 0
        ),
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if bool(result["pass"]) else 1


if __name__ == "__main__":
    sys.exit(main())
