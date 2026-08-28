"""Shared Redis state for horizontally scaled MCCxBot processes.

Only ephemeral coordination lives here. MongoDB remains authoritative for
movie rows, configuration, checkpoints, and durable jobs.
"""

from __future__ import annotations

import hashlib
import os
import secrets
import threading
import time
from typing import Any

from bson import json_util
from redis.asyncio import Redis
from redis.asyncio.connection import ConnectionPool


class RedisConfigurationError(RuntimeError):
    """Raised when shared state is required but Redis is not configured."""


_SEMAPHORE_ACQUIRE = """
local key = KEYS[1]
local now = tonumber(ARGV[1])
local expires = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
local token = ARGV[4]
redis.call('ZREMRANGEBYSCORE', key, '-inf', now)
if redis.call('ZCARD', key) >= limit then
  return 0
end
redis.call('ZADD', key, expires, token)
redis.call('PEXPIRE', key, math.max(1000, (expires - now) * 2))
return 1
"""

_COUNTER_INCREMENT = """
local value = redis.call('INCRBY', KEYS[1], ARGV[1])
redis.call('EXPIRE', KEYS[1], ARGV[2])
return value
"""

_COUNTER_DECREMENT = """
local current = tonumber(redis.call('GET', KEYS[1]) or '0')
if current <= 1 then
  redis.call('DEL', KEYS[1])
  return 0
end
local value = redis.call('DECR', KEYS[1])
redis.call('EXPIRE', KEYS[1], ARGV[1])
return value
"""


def stable_cache_key(*parts: Any) -> str:
    payload = "\x1f".join(str(part) for part in parts).encode("utf-8", "surrogatepass")
    return hashlib.sha256(payload).hexdigest()


class RedisState:
    """Small typed facade over an async Redis connection pool."""

    def __init__(self, url: str | None = None, *, prefix: str | None = None, client=None):
        self.url = (url if url is not None else os.getenv("REDIS_URL", "")).strip()
        self.prefix = (prefix or os.getenv("REDIS_PREFIX", "mccxbot")).strip(":")
        self._memory_mode = self.url == "memory://"
        self._memory: dict[str, tuple[float, Any]] = {}
        self._memory_hashes: dict[str, tuple[float, dict[str, Any]]] = {}
        self._memory_semaphores: dict[str, dict[str, float]] = {}
        self._memory_lock = threading.RLock()
        self._pool = None
        self._client = client
        if self._client is None and self.url and not self._memory_mode:
            max_connections = max(20, min(500, int(os.getenv("REDIS_MAX_CONNECTIONS", "100"))))
            self._pool = ConnectionPool.from_url(
                self.url,
                max_connections=max_connections,
                decode_responses=False,
                socket_connect_timeout=3,
                socket_timeout=3,
                health_check_interval=30,
            )
            self._client = Redis(connection_pool=self._pool)

    @property
    def configured(self) -> bool:
        return self._memory_mode or self._client is not None

    def require_configured(self):
        if not self.configured:
            raise RedisConfigurationError(
                "REDIS_URL is required for shared sessions, coordination, and horizontal scaling."
            )

    def key(self, namespace: str, key: Any) -> str:
        namespace = str(namespace).strip(":")
        return f"{self.prefix}:{namespace}:{key}"

    async def start(self):
        self.require_configured()
        if self._memory_mode:
            if os.getenv("MCCX_ALLOW_MEMORY_REDIS_FOR_TESTS") != "1":
                raise RedisConfigurationError(
                    "memory:// Redis is permitted only by the hermetic unit-test suite."
                )
            return
        await self._client.ping()

    async def close(self):
        if self._memory_mode:
            return
        if self._client is not None:
            await self._client.aclose()
        if self._pool is not None:
            await self._pool.aclose()

    async def set_json(self, namespace: str, key: Any, value: Any, ttl: int):
        payload = json_util.dumps(value, separators=(",", ":")).encode("utf-8")
        if self._memory_mode:
            with self._memory_lock:
                self._memory[self.key(namespace, key)] = (
                    time.time() + max(1, int(ttl)),
                    payload,
                )
            return
        await self._client.set(self.key(namespace, key), payload, ex=max(1, int(ttl)))

    async def get_json(self, namespace: str, key: Any, default=None):
        if self._memory_mode:
            with self._memory_lock:
                entry = self._memory.get(self.key(namespace, key))
                if entry and entry[0] <= time.time():
                    self._memory.pop(self.key(namespace, key), None)
                    entry = None
                payload = entry[1] if entry else None
        else:
            payload = await self._client.get(self.key(namespace, key))
        if payload is None:
            return default
        if isinstance(payload, bytes):
            payload = payload.decode("utf-8")
        return json_util.loads(payload)

    async def delete(self, namespace: str, key: Any) -> bool:
        if self._memory_mode:
            with self._memory_lock:
                return self._memory.pop(self.key(namespace, key), None) is not None
        return bool(await self._client.delete(self.key(namespace, key)))

    async def claim_once(self, namespace: str, key: Any, ttl: int) -> bool:
        if self._memory_mode:
            redis_key = self.key(namespace, key)
            with self._memory_lock:
                entry = self._memory.get(redis_key)
                if entry and entry[0] > time.time():
                    return False
                self._memory[redis_key] = (time.time() + max(1, int(ttl)), b"1")
                return True
        return bool(
            await self._client.set(
                self.key(namespace, key),
                b"1",
                ex=max(1, int(ttl)),
                nx=True,
            )
        )

    async def cooldown(self, namespace: str, key: Any, ttl: int) -> int:
        redis_key = self.key(namespace, key)
        if self._memory_mode:
            with self._memory_lock:
                entry = self._memory.get(redis_key)
                if not entry or entry[0] <= time.time():
                    self._memory[redis_key] = (time.time() + max(1, int(ttl)), time.time())
                    return 0
                return max(1, int(entry[0] - time.time()))
        claimed = await self._client.set(redis_key, str(time.time()), ex=max(1, int(ttl)), nx=True)
        if claimed:
            return 0
        remaining = await self._client.ttl(redis_key)
        return max(1, int(remaining))

    async def acquire_semaphore(
        self,
        name: str,
        limit: int,
        lease_seconds: int,
    ) -> str | None:
        token = secrets.token_hex(16)
        now_ms = int(time.time() * 1000)
        expires_ms = now_ms + max(1, int(lease_seconds)) * 1000
        if self._memory_mode:
            redis_key = self.key("semaphore", name)
            with self._memory_lock:
                leases = self._memory_semaphores.setdefault(redis_key, {})
                leases = {key: expiry for key, expiry in leases.items() if expiry > now_ms}
                self._memory_semaphores[redis_key] = leases
                if len(leases) >= max(1, int(limit)):
                    return None
                leases[token] = expires_ms
                return token
        acquired = await self._client.eval(
            _SEMAPHORE_ACQUIRE,
            1,
            self.key("semaphore", name),
            now_ms,
            expires_ms,
            max(1, int(limit)),
            token,
        )
        return token if int(acquired or 0) == 1 else None

    async def release_semaphore(self, name: str, token: str):
        if self._memory_mode:
            with self._memory_lock:
                self._memory_semaphores.get(self.key("semaphore", name), {}).pop(token, None)
            return
        await self._client.zrem(self.key("semaphore", name), token)

    async def increment_counter(self, name: str, *, amount: int = 1, ttl: int = 30) -> int:
        if self._memory_mode:
            redis_key = self.key("counter", name)
            with self._memory_lock:
                entry = self._memory.get(redis_key)
                current = int(entry[1]) if entry and entry[0] > time.time() else 0
                current += int(amount)
                self._memory[redis_key] = (time.time() + max(1, int(ttl)), current)
                return current
        return int(
            await self._client.eval(
                _COUNTER_INCREMENT,
                1,
                self.key("counter", name),
                int(amount),
                max(1, int(ttl)),
            )
        )

    async def decrement_counter(self, name: str, *, ttl: int = 30) -> int:
        if self._memory_mode:
            redis_key = self.key("counter", name)
            with self._memory_lock:
                entry = self._memory.get(redis_key)
                current = int(entry[1]) if entry and entry[0] > time.time() else 0
                current = max(0, current - 1)
                if current:
                    self._memory[redis_key] = (time.time() + max(1, int(ttl)), current)
                else:
                    self._memory.pop(redis_key, None)
                return current
        return int(
            await self._client.eval(
                _COUNTER_DECREMENT,
                1,
                self.key("counter", name),
                max(1, int(ttl)),
            )
        )

    async def get_counter(self, name: str) -> int:
        if self._memory_mode:
            redis_key = self.key("counter", name)
            with self._memory_lock:
                entry = self._memory.get(redis_key)
                if not entry or entry[0] <= time.time():
                    self._memory.pop(redis_key, None)
                    return 0
                return int(entry[1])
        value = await self._client.get(self.key("counter", name))
        return int(value or 0)

    async def set_hash_json(self, namespace: str, field: Any, value: Any, ttl: int = 60):
        redis_key = self.key("hash", namespace)
        payload = json_util.dumps(value, separators=(",", ":")).encode("utf-8")
        if self._memory_mode:
            with self._memory_lock:
                entry = self._memory_hashes.get(redis_key)
                values = dict(entry[1]) if entry and entry[0] > time.time() else {}
                values[str(field)] = payload
                self._memory_hashes[redis_key] = (
                    time.time() + max(1, int(ttl)), values
                )
            return
        pipeline = self._client.pipeline(transaction=False)
        pipeline.hset(redis_key, str(field), payload)
        pipeline.expire(redis_key, max(1, int(ttl)))
        await pipeline.execute()

    async def get_hash_json(self, namespace: str) -> dict[str, Any]:
        if self._memory_mode:
            redis_key = self.key("hash", namespace)
            with self._memory_lock:
                entry = self._memory_hashes.get(redis_key)
                if not entry or entry[0] <= time.time():
                    self._memory_hashes.pop(redis_key, None)
                    values = {}
                else:
                    values = dict(entry[1])
        else:
            values = await self._client.hgetall(self.key("hash", namespace))
        decoded = {}
        for field, payload in values.items():
            if isinstance(field, bytes):
                field = field.decode("utf-8")
            if isinstance(payload, bytes):
                payload = payload.decode("utf-8")
            decoded[str(field)] = json_util.loads(payload)
        return decoded


redis_state = RedisState()
