import ast
import asyncio
import unittest
from pathlib import Path

from plugins.access_policy import authorize_user_action


ROOT = Path(__file__).resolve().parents[1]


class FakeRepository:
    def __init__(self, *, banned=False, config=None):
        self.banned = banned
        self.config = config or {}
        self.config_reads = 0

    async def is_banned(self, _user_id):
        return self.banned

    async def get_config(self):
        self.config_reads += 1
        return self.config


class AccessPolicyTests(unittest.TestCase):
    def run_policy(self, repository, *, user_id=10, config=None, admin_ids=()):
        return asyncio.run(
            authorize_user_action(
                user_id,
                "test",
                config,
                repository=repository,
                admin_ids=admin_ids,
            )
        )

    def test_ban_always_denies(self):
        decision = self.run_policy(FakeRepository(banned=True), admin_ids=(10,))
        self.assertFalse(decision.allowed)
        self.assertEqual(decision.reason, "banned")

    def test_maintenance_denies_non_admin_and_allows_admin(self):
        repository = FakeRepository(config={"maintenance_mode": True})
        self.assertFalse(self.run_policy(repository, admin_ids=(99,)).allowed)
        self.assertTrue(self.run_policy(repository, admin_ids=(10,)).allowed)

    def test_supplied_config_avoids_another_config_read(self):
        repository = FakeRepository()
        decision = self.run_policy(repository, config={}, admin_ids=())
        self.assertTrue(decision.allowed)
        self.assertEqual(repository.config_reads, 0)

    def test_all_public_boundaries_call_the_central_policy(self):
        expected = {
            "plugins/filter.py": {
                "auto_filter", "handle_pagination", "handle_expand_series",
                "show_filter_menu", "apply_result_filter", "clear_result_filters",
                "send_movie_file", "check_fsub_callback",
            },
            "plugins/start.py": {"_execute_search", "_handle_file_link", "start_handler"},
            "plugins/req_fsub.py": {
                "_deliver_file", "check_verification_gates", "vgate_check_callback",
            },
            "plugins/request.py": {"send_request_ticket"},
            "plugins/group_connect.py": {"group_search", "handle_group_pagination"},
        }
        policy_calls = {"authorize_user_action", "enforce_user_action"}
        for relative_path, function_names in expected.items():
            tree = ast.parse((ROOT / relative_path).read_text(encoding="utf-8"))
            functions = {
                node.name: node
                for node in ast.walk(tree)
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            }
            for function_name in function_names:
                calls = {
                    node.func.id
                    for node in ast.walk(functions[function_name])
                    if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                }
                self.assertTrue(
                    calls & policy_calls,
                    f"{relative_path}:{function_name} bypasses the access policy",
                )


if __name__ == "__main__":
    unittest.main()
