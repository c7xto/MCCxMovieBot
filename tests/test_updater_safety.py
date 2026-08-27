import tempfile
import unittest
from pathlib import Path

from plugins.updater import _github_slug, _safe_relative, _safe_target, _skip


class UpdaterSafetyTests(unittest.TestCase):
    def test_repo_url_is_normalized(self):
        self.assertEqual(
            _github_slug("https://github.com/c7xto/MCCxMovieBot.git"),
            "c7xto/MCCxMovieBot",
        )

    def test_unsafe_relative_paths_are_rejected(self):
        for value in ("../secret", "/tmp/secret", "C:/secret", "a/../secret", ""):
            with self.subTest(value=value), self.assertRaises(RuntimeError):
                _safe_relative(value)

    def test_runtime_and_secret_paths_are_skipped(self):
        for value in (
            ".env",
            "runtime/ready",
            ".git/config",
            "MCCxBot.session",
            "nested/MCCxBot.session-journal",
        ):
            with self.subTest(value=value):
                self.assertTrue(_skip(value))

    def test_existing_symlink_component_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            outside = root.parent / f"outside-{root.name}"
            outside.mkdir(exist_ok=True)
            link = root / "linked"
            try:
                link.symlink_to(outside, target_is_directory=True)
            except OSError:
                self.skipTest("Symlink creation is unavailable")
            try:
                with self.assertRaises(RuntimeError):
                    _safe_target(root, "linked/file.py")
            finally:
                link.unlink(missing_ok=True)
                outside.rmdir()

    def test_updater_keeps_confirmation_and_locked_dependencies(self):
        source = (Path(__file__).resolve().parents[1] / "plugins" / "updater.py").read_text(
            encoding="utf-8"
        )
        self.assertIn("--require-hashes", source)
        self.assertIn("compare/{GITHUB_BRANCH}", source)
        self.assertIn("_safe_target(PROJECT_ROOT", source)
        self.assertIn('key="maintenance:self-update"', source)


if __name__ == "__main__":
    unittest.main()
