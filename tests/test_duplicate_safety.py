import unittest
from pathlib import Path

from plugins.duplicate_safety import (
    select_keep_and_delete,
    stable_duplicate_key,
)


ROOT = Path(__file__).resolve().parents[1]


class DuplicateSafetyTests(unittest.TestCase):
    def test_oldest_object_id_is_never_in_delete_list(self):
        oldest = "64a000000000000000000001"
        newer = [
            "64b000000000000000000001",
            "64c000000000000000000001",
        ]
        keep, delete = select_keep_and_delete([newer[1], oldest, newer[0]])
        self.assertEqual(keep, oldest)
        self.assertEqual(set(delete), set(newer))
        self.assertNotIn(keep, delete)

    def test_selection_is_idempotent_with_repeated_ids(self):
        keep, delete = select_keep_and_delete(
            [
                "64a000000000000000000001",
                "64b000000000000000000001",
                "64b000000000000000000001",
            ]
        )
        self.assertEqual(keep, "64a000000000000000000001")
        self.assertEqual(delete, ["64b000000000000000000001"])

    def test_group_token_is_stable_and_callback_safe(self):
        key = stable_duplicate_key("exact", "telegram-file-id")
        self.assertEqual(key, stable_duplicate_key("exact", "telegram-file-id"))
        self.assertLessEqual(len(f"fm_dupe_delete#{key}".encode()), 64)

    def test_callbacks_never_embed_object_id_lists(self):
        source = (ROOT / "plugins/file_manager.py").read_text(encoding="utf-8")
        self.assertNotIn('ids_joined = ",".join', source)
        self.assertNotIn("fm_del_dupes#", source)


if __name__ == "__main__":
    unittest.main()
