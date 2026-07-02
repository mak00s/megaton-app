"""Fire-and-forget webhook notification.

Pipelines write their results elsewhere (Sheets, BQ); this pushes a small
"needs attention" digest to a user-configured webhook (Make.com, Slack, …)
which routes it onward, keeping scenario wiring out of the codebase.

Design rules:
  • stdlib only (urllib) — no new dependency for one POST.
  • Never raises: a notification failure must not fail a run that has already
    written correct data. Returns False and logs instead.
"""

from __future__ import annotations

import json
import logging
import urllib.error
import urllib.request

logger = logging.getLogger(__name__)

_TIMEOUT_S = 10


def post_webhook(url: str, payload: dict, *, timeout_s: int = _TIMEOUT_S) -> bool:
    """POST ``payload`` as JSON. Returns True on 2xx; never raises."""
    if not url:
        return False
    try:
        # Serialization inside the guard: a circular reference (ValueError) or
        # a non-str dict key (TypeError) must fail the notification, not the
        # run that produced the payload.
        body = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
        req = urllib.request.Request(
            url, data=body, headers={"Content-Type": "application/json"}, method="POST"
        )
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:  # noqa: S310 - user-configured URL
            ok = 200 <= resp.status < 300
            if not ok:
                logger.warning("webhook returned HTTP %s", resp.status)
            return ok
    except (urllib.error.URLError, OSError, ValueError, TypeError) as exc:
        logger.warning("webhook POST failed: %s", exc)
        return False
