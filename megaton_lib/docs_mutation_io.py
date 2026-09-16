"""Private immutable plans and durable receipts, never stdout logging."""
from __future__ import annotations

import json
import os
from pathlib import Path
import tempfile

from .docs_client import DocsEditError


def create_receipt(path, value):
    """Exclusive creation is also a local replay guard; keep the file on failure."""
    path = Path(path)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2)
        stream.flush()
        os.fsync(stream.fileno())


def save_receipt(path, value):
    path = Path(path)
    if not path.is_file() or path.is_symlink():
        raise DocsEditError("Receipt must be an existing private regular file.")
    fd, tmp = tempfile.mkstemp(prefix=".docs-receipt-", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(value, stream, ensure_ascii=False, indent=2)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)
