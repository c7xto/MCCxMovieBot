"""Container readiness probe with no network or secret access."""

import os
import time
from pathlib import Path


MAX_MARKER_AGE_SECONDS = 30 * 60


def main() -> int:
    marker = Path(os.getenv("SESSION_WORKDIR", "/app/runtime")) / "ready"
    try:
        age = time.time() - marker.stat().st_mtime
        os.kill(1, 0)
    except (OSError, ValueError):
        return 1
    return 0 if 0 <= age <= MAX_MARKER_AGE_SECONDS else 1


if __name__ == "__main__":
    raise SystemExit(main())
