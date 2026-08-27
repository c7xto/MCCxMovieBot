import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class DockerContextSecurityTests(unittest.TestCase):
    def test_runtime_credentials_are_excluded_from_build_context(self):
        rules = {
            line.strip()
            for line in (ROOT / ".dockerignore").read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        }
        required = {
            ".env",
            "*.session",
            "*.session-journal",
            "runtime/",
            ".deployed_sha",
            ".deployed_files.json",
        }
        self.assertTrue(
            required <= rules,
            f"Docker build context is missing credential exclusions: {required - rules}",
        )


if __name__ == "__main__":
    unittest.main()
