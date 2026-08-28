"""Hermetic shared-state configuration for the unit-test process."""

import os

import pytest


os.environ.setdefault("REDIS_URL", "memory://")
os.environ.setdefault("MCCX_ALLOW_MEMORY_REDIS_FOR_TESTS", "1")


@pytest.fixture(autouse=True)
def clear_memory_redis():
    from database.redis_client import redis_state

    redis_state._memory.clear()
    redis_state._memory_hashes.clear()
    redis_state._memory_semaphores.clear()
    yield
    redis_state._memory.clear()
    redis_state._memory_hashes.clear()
    redis_state._memory_semaphores.clear()
