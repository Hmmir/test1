import os
import urllib.request
from typing import Any, Optional, Union


# This repo often runs in environments where HTTP(S)_PROXY is set globally.
# By default we *ignore* environment proxies to keep WB/Google/local requests working.
# Set BTLZ_USE_ENV_PROXY=1 to respect env proxy variables.
USE_ENV_PROXY = str(os.environ.get("BTLZ_USE_ENV_PROXY", "0") or "").strip().lower() in {
    "1",
    "true",
    "yes",
}


_OPENER = (
    urllib.request.build_opener()
    if USE_ENV_PROXY
    else urllib.request.build_opener(urllib.request.ProxyHandler({}))
)


def open_url(
    req: Union[str, urllib.request.Request],
    timeout: Optional[float] = None,
    **kwargs: Any,
):
    """Open a URL/Request via urllib, optionally ignoring env proxies (default)."""

    if timeout is None:
        return _OPENER.open(req, **kwargs)
    return _OPENER.open(req, timeout=timeout, **kwargs)

