import os
import stat
import tempfile
import unittest
from pathlib import Path

from plugins.process_lock import (
    AlreadyRunningError,
    ProcessLockError,
    acquire_process_lock,
    prepare_private_runtime_dir,
)


ROOT = Path(__file__).resolve().parents[1]


class ProcessLockTests(unittest.TestCase):
    def test_lock_is_regular_private_runtime_state_and_is_exclusive(self):
        with tempfile.TemporaryDirectory() as temporary:
            runtime = Path(temporary) / "runtime"
            first = acquire_process_lock(runtime)
            try:
                lock_path = runtime / "mccxbot.lock"
                self.assertTrue(lock_path.is_file())
                self.assertFalse(lock_path.is_symlink())
                with self.assertRaises(AlreadyRunningError):
                    acquire_process_lock(runtime)
                if os.name != "nt":
                    self.assertEqual(
                        stat.S_IMODE(runtime.stat().st_mode), 0o700
                    )
                    self.assertEqual(
                        stat.S_IMODE(lock_path.stat().st_mode), 0o600
                    )
            finally:
                first.close()
            replacement = acquire_process_lock(runtime)
            replacement.close()

    def test_non_regular_lock_target_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            runtime = prepare_private_runtime_dir(Path(temporary) / "runtime")
            (runtime / "mccxbot.lock").mkdir()
            with self.assertRaises(ProcessLockError):
                acquire_process_lock(runtime)

    def test_symlink_runtime_directory_is_rejected_when_supported(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            target = root / "target"
            target.mkdir()
            link = root / "runtime-link"
            try:
                link.symlink_to(target, target_is_directory=True)
            except OSError:
                self.skipTest("symbolic links are unavailable")
            with self.assertRaises(ProcessLockError):
                prepare_private_runtime_dir(link)

    def test_bot_does_not_use_shared_temp_or_append_mode(self):
        source = (ROOT / "bot.py").read_text(encoding="utf-8")
        self.assertNotIn("tempfile.gettempdir", source)
        self.assertNotIn('open(_lock_path, "a+")', source)
        self.assertIn("acquire_process_lock(SESSION_WORKDIR)", source)


if __name__ == "__main__":
    unittest.main()
