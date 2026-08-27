"""Run the same reproducible quality gate used by CI."""

import argparse
import os
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def run(*args, env=None):
    subprocess.run(args, cwd=ROOT, check=True, env=env)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--skip-install",
        action="store_true",
        help="Use the already-installed locked development dependencies.",
    )
    args = parser.parse_args()
    python = sys.executable
    if not args.skip_install:
        run(python, "-m", "pip", "install", "-r", "requirements-dev.txt")
    run(
        python,
        "-m",
        "compileall",
        "-q",
        "database",
        "plugins",
        "tests",
        "tools",
        "bot.py",
        "tmdb.py",
        "utils.py",
        "verification.py",
    )
    run(python, "-m", "ruff", "check", ".")
    run(python, "-m", "pytest", "-q", "-p", "no:cacheprovider")
    pip_cache = ROOT / "runtime" / "pip-cache"
    pip_cache.mkdir(parents=True, exist_ok=True)
    audit_environment = os.environ.copy()
    audit_environment["PIP_CACHE_DIR"] = str(pip_cache)
    run(
        python,
        "-m",
        "pip_audit",
        "-r",
        "requirements.txt",
        "--cache-dir",
        str(pip_cache),
        "--progress-spinner",
        "off",
        env=audit_environment,
    )


if __name__ == "__main__":
    main()
