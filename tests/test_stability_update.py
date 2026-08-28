import ast
import asyncio
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from database.shard_router import ShardRouter
from database.db import Database
from plugins import req_fsub
from plugins.access_gates import get_access_gates, legacy_access_gates
from plugins.callbacks import answer_callback_safely
from plugins.workload import background_turn, interactive_slot, workload_snapshot
from verification import VerificationResult, VerificationStatus


ROOT = Path(__file__).resolve().parents[1]


class _AsyncCursor:
    def __init__(self, documents):
        self.documents = list(documents)

    def limit(self, amount):
        self.documents = self.documents[:amount]
        return self

    def __aiter__(self):
        self._iterator = iter(self.documents)
        return self

    async def __anext__(self):
        try:
            return next(self._iterator)
        except StopIteration as error:
            raise StopAsyncIteration from error


class _RegistryCollection:
    def __init__(self, documents):
        self.documents = documents
        self.find_calls = 0
        self.bulk_calls = 0

    def find(self, *_args, **_kwargs):
        self.find_calls += 1
        return _AsyncCursor(self.documents)

    async def bulk_write(self, operations, ordered=False):
        self.bulk_calls += 1
        return SimpleNamespace(modified_count=len(operations))


class _MovieCollection:
    def __init__(self, documents):
        self.documents = documents
        self.find_calls = 0

    def find(self, query, *_args, **_kwargs):
        self.find_calls += 1
        wanted = set(query["file_id"]["$in"])
        return _AsyncCursor(
            document for document in self.documents if document["file_id"] in wanted
        )


def test_legacy_access_gates_merge_duplicates_and_keep_required_stricter():
    gates = legacy_access_gates(
        {
            "fsub_channels": [{"id": -1001, "link": "https://t.me/required"}],
            "req_fsub_channels": [
                {"id": -1001, "link": "https://t.me/timed"},
                {"id": -1002, "link": "https://t.me/second"},
            ],
            "req_fsub_interval_hours": 12,
            "two_stage_channels": [],
        }
    )
    assert len(gates) == 2
    assert next(g for g in gates if g["id"] == -1001)["mode"] == "required"
    assert next(g for g in gates if g["id"] == -1002)["interval_seconds"] == 12 * 3600


def test_canonical_access_gates_are_read_without_legacy_fields():
    config = {
        "access_gates_schema_version": 1,
        "access_gates": [
            {
                "id": -1003,
                "mode": "timed",
                "interval_seconds": 3600,
                "enabled": True,
            }
        ],
    }
    gates = get_access_gates(config)
    assert len(gates) == 1
    assert gates[0]["id"] == -1003


def test_malformed_canonical_gate_fails_closed_as_an_invalid_gate():
    gates = get_access_gates(
        {"access_gates_schema_version": 1, "access_gates": ["broken"]}
    )
    assert len(gates) == 1
    assert gates[0]["id"] is None


@pytest.mark.asyncio
async def test_interactive_load_gate_defers_background_but_never_starves_it():
    async with interactive_slot("test_interaction"):
        observed_idle = await background_turn("test_background", max_defer=0.01)
        assert observed_idle is False
    assert await background_turn("test_background", max_defer=0.01) is True
    assert workload_snapshot()["background_fairness_release:test_background"] >= 1


def test_shard_router_redirects_writes_and_recovers_after_probe():
    router = ShardRouter(3)
    router.record_size(0, 451)
    router.mark_error(1, RuntimeError("writes are blocked: over your space quota"))
    assert router.candidates() == [2]
    router.record_size(1, 200)
    assert router.candidates() == [1, 2]
    assert router.snapshot()[0]["state"] == "full"


def test_shard_router_keeps_full_shards_readable_and_outages_out_of_fanout():
    router = ShardRouter(3)
    for index in range(3):
        router.mark_reachable(index)
    router.record_size(0, 451)
    router.mark_unavailable(1, "ServerSelectionTimeoutError")

    assert router.candidates() == [2]
    assert router.read_candidates() == [0, 2]
    assert router.unavailable() == [2]

    router.mark_reachable(1, "manual_health_check")
    assert router.read_candidates() == [0, 1, 2]


def test_unknown_capacity_blocks_writes_without_hiding_readable_files():
    router = ShardRouter(1)
    router.mark_reachable(0)
    router.record_size(0, float("inf"))

    assert router.candidates() == []
    assert router.read_candidates() == [0]
    assert router.unavailable() == []


@pytest.mark.asyncio
async def test_live_file_counter_keeps_last_known_value_when_one_shard_fails():
    database = Database.__new__(Database)
    database.file_cols = [
        SimpleNamespace(estimated_document_count=AsyncMock(return_value=10)),
        SimpleNamespace(
            estimated_document_count=AsyncMock(side_effect=ConnectionError("offline"))
        ),
    ]
    database.shard_router = ShardRouter(2)
    database.shard_router.mark_reachable(0)
    database.shard_router.mark_reachable(1)
    database._file_count_cache = (0.0, 0)
    database._cluster_file_count_cache = {1: 55}

    assert await database.get_total_files() == 65
    assert database.unavailable_shards() == [2]


@pytest.mark.asyncio
async def test_registry_reconciliation_batches_one_lookup_per_readable_shard():
    registry = _RegistryCollection(
        [
            {"_id": "registry-a", "file_id": "file-a"},
            {"_id": "registry-b", "file_id": "file-b"},
        ]
    )
    movie_one = _MovieCollection([{"_id": "movie-a", "file_id": "file-a"}])
    movie_two = _MovieCollection([{"_id": "movie-b", "file_id": "file-b"}])
    database = Database.__new__(Database)
    database.registry_col = registry
    database.file_cols = [movie_one, movie_two]
    database.shard_router = ShardRouter(2)
    database.shard_router.mark_reachable(0)
    database.shard_router.mark_reachable(1)

    result = await database.reconcile_registry_locations(limit=250)

    assert result == {"checked": 2, "repaired": 2, "unresolved": 0}
    assert registry.find_calls == 1
    assert registry.bulk_calls == 1
    assert movie_one.find_calls == 1
    assert movie_two.find_calls == 1


@pytest.mark.asyncio
async def test_callback_answer_is_sent_only_once():
    callback = SimpleNamespace(id="query-1", data="button", answer=AsyncMock())
    assert await answer_callback_safely(callback)
    assert await answer_callback_safely(callback, "second")
    callback.answer.assert_awaited_once()


class _GateRepository:
    def __init__(self, cache, membership_result):
        self.cache = cache
        self.membership_result = membership_result
        self.invalidated = []

    async def get_config(self):
        return {
            "access_gates_schema_version": 1,
            "access_gates": [
                {
                    "key": "gate-one",
                    "id": -1001,
                    "link": "https://t.me/gate",
                    "label": "Gate",
                    "mode": "required",
                    "interval_seconds": 900,
                    "enabled": True,
                }
            ],
        }

    async def get_verification_cache(self, _user_id, _keys):
        return self.cache

    async def invalidate_gate_verification(self, user_id, gate_key):
        self.invalidated.append((user_id, gate_key))

    async def mark_gate_verified(self, *_args):
        return True


class _GateClient:
    async def get_chat(self, _channel_id):
        return SimpleNamespace(title="Gate")


@pytest.mark.asyncio
async def test_recent_verification_grace_handles_only_indeterminate_failures():
    now = time.time()
    repository = _GateRepository(
        {"gate-one": {"valid_until": now - 1, "grace_until": now + 60}},
        VerificationResult.indeterminate("telegram_timeout"),
    )
    with (
        patch.object(req_fsub, "db", repository),
        patch.object(
            req_fsub,
            "_requested_or_joined_status",
            AsyncMock(return_value=repository.membership_result),
        ),
    ):
        evaluation = await req_fsub._collect_outstanding_gates(_GateClient(), 7)
    assert evaluation.result.status is VerificationStatus.PASS


@pytest.mark.asyncio
async def test_definite_membership_denial_invalidates_cache():
    now = time.time()
    repository = _GateRepository(
        {"gate-one": {"valid_until": now - 1, "grace_until": now + 60}},
        VerificationResult.deny("not_joined"),
    )
    with (
        patch.object(req_fsub, "db", repository),
        patch.object(
            req_fsub,
            "_requested_or_joined_status",
            AsyncMock(return_value=repository.membership_result),
        ),
    ):
        evaluation = await req_fsub._collect_outstanding_gates(_GateClient(), 7)
    assert evaluation.result.status is VerificationStatus.DENY
    assert repository.invalidated == [(7, "gate-one")]


@pytest.mark.asyncio
async def test_legacy_cache_failure_falls_back_to_direct_membership_check():
    repository = _GateRepository({}, VerificationResult.allow("joined"))

    async def unavailable_legacy_cache(_user_id):
        return VerificationResult.indeterminate("legacy_database_unavailable")

    repository.get_req_fsub_gate_status = unavailable_legacy_cache

    async def timed_config():
        return {
            "access_gates_schema_version": 1,
            "access_gates": [
                {
                    "key": "gate-one",
                    "id": -1001,
                    "link": "https://t.me/gate",
                    "label": "Gate",
                    "mode": "timed",
                    "interval_seconds": 900,
                    "enabled": True,
                    "source": "request_fsub",
                }
            ],
        }

    repository.get_config = timed_config
    with (
        patch.object(req_fsub, "db", repository),
        patch.object(
            req_fsub,
            "_requested_or_joined_status",
            AsyncMock(return_value=repository.membership_result),
        ) as membership,
    ):
        evaluation = await req_fsub._collect_outstanding_gates(_GateClient(), 7)
    assert evaluation.result.status is VerificationStatus.PASS
    membership.assert_awaited_once()


@pytest.mark.asyncio
async def test_membership_timeout_uses_recent_grace(monkeypatch):
    now = time.time()
    repository = _GateRepository(
        {"gate-one": {"valid_until": now - 1, "grace_until": now + 60}},
        VerificationResult.allow("unused"),
    )

    async def slow_membership(*_args, **_kwargs):
        await asyncio.sleep(1)

    monkeypatch.setattr(req_fsub, "_VERIFICATION_IO_TIMEOUT", 0.01)
    with (
        patch.object(req_fsub, "db", repository),
        patch.object(req_fsub, "_requested_or_joined_status", slow_membership),
    ):
        evaluation = await req_fsub._collect_outstanding_gates(_GateClient(), 7)
    assert evaluation.result.status is VerificationStatus.PASS


def test_all_callback_handlers_acknowledge_before_io():
    accepted = {
        "answer_callback_safely",
        "enforce_user_action",
        "begin_prompt",
    }
    for path in (ROOT / "plugins").glob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for function in [node for node in tree.body if isinstance(node, ast.AsyncFunctionDef)]:
            callback_handler = any(
                isinstance(decorator, ast.Call)
                and isinstance(decorator.func, ast.Attribute)
                and decorator.func.attr == "on_callback_query"
                for decorator in function.decorator_list
            )
            if not callback_handler:
                continue
            awaits = [node for node in ast.walk(function) if isinstance(node, ast.Await)]
            if not awaits:
                continue
            first = min(awaits, key=lambda node: (node.lineno, node.col_offset))
            called = first.value.func if isinstance(first.value, ast.Call) else None
            name = (
                called.id
                if isinstance(called, ast.Name)
                else called.attr if isinstance(called, ast.Attribute) else ""
            )
            assert name in accepted, f"{path.name}:{function.name} performs I/O before callback ACK"


def test_dedicated_operations_migration_is_non_destructive():
    tree = ast.parse((ROOT / "database/db.py").read_text(encoding="utf-8"))
    function = next(
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "_migrate_to_dedicated_operations"
    )
    destructive = {
        node.func.attr
        for node in ast.walk(function)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
        and node.func.attr in {"delete_one", "delete_many", "drop"}
    }
    assert destructive == set()
    assert "legacy_data_retained" in ast.unparse(function)


def test_private_search_rejects_any_slash_command_before_database_io():
    tree = ast.parse((ROOT / "plugins/filter.py").read_text(encoding="utf-8"))
    function = next(
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "auto_filter"
    )
    first_statement = function.body[0]
    assert isinstance(first_statement, ast.If)
    assert "startswith('/')" in ast.unparse(first_statement.test).replace('"', "'")
    assert any(isinstance(node, ast.Return) for node in first_statement.body)
    first_await = min(node.lineno for node in ast.walk(function) if isinstance(node, ast.Await))
    assert first_statement.lineno < first_await
