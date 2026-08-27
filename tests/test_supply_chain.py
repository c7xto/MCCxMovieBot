import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class SupplyChainTests(unittest.TestCase):
    def test_ci_actions_are_commit_pinned(self):
        workflow = (ROOT / ".github" / "workflows" / "quality.yml").read_text(
            encoding="utf-8"
        )
        self.assertNotIn("actions/checkout@v", workflow)
        self.assertNotIn("actions/setup-python@v", workflow)
        self.assertIn("permissions:\n      contents: read", workflow)

    def test_container_image_and_dependencies_are_immutable(self):
        dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
        self.assertRegex(dockerfile.splitlines()[0], r"@sha256:[0-9a-f]{64}$")
        self.assertIn("--require-hashes -r requirements.lock", dockerfile)
        self.assertTrue((ROOT / "requirements.lock").is_file())
        self.assertTrue((ROOT / "requirements-dev.lock").is_file())

    def test_compose_has_runtime_limits_and_readiness(self):
        compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
        for value in (
            "read_only: true",
            "cap_drop:",
            "no-new-privileges:true",
            "pids_limit:",
            "mem_limit:",
            "healthcheck:",
            "bot-runtime:/app/runtime",
        ):
            with self.subTest(value=value):
                self.assertIn(value, compose)


if __name__ == "__main__":
    unittest.main()
