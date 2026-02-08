from __future__ import annotations

import sys
from pathlib import Path

# Some environments invoke `pytest` via an installed entrypoint script. In that
# case, the project root is not guaranteed to be on sys.path, and imports like
# `import app` / `import scripts` / `import lib` may fail. Ensure the repo root
# is importable for tests.
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

