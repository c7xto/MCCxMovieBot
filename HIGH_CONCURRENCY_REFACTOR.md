# MCCx Movie Bot — High-Concurrency Refactor

## Architecture summary

This release separates the user-facing bot from durable background work while
retaining an `all-in-one` role for single-process hosting. Redis stores only
short-lived coordination data. MongoDB remains authoritative for files,
configuration, access rules, the file registry, and durable jobs.

| Area | Production implementation |
|---|---|
| Startup | MongoDB validation, migrations, required indexes, registry checks, and Redis ping complete before Kurigram connects. A highest-priority readiness gate rejects the remaining connection window. |
| MongoDB | Five optional movie shards use bounded pools with `waitQueueTimeoutMS=5000` and `maxConnecting=10`. A separate `OPERATIONS_DATABASE_URI` is mandatory. |
| Redis | Search sessions, query pages, admin prompts, callback/update deduplication, broadcast previews, duplicate report pages, request cooldowns, announcement cooldowns, interactive load, global search admission, and shard health are shared. |
| Search | User search uses the `search_tokens` multikey index only. Filename regex fallback and MongoDB `skip()` pagination were removed. Per-shard `_id` keyset cursors provide deterministic continuation. |
| Analytics | File language totals use atomic counters in the Operations database rather than `$facet` regex scans across all movie shards. |
| Telegram | Callback acknowledgement happens before Redis or MongoDB work. FloodWait handling is centralized and Kurigram's automatic sleep threshold is 10 seconds. |
| Workers | `bot-interactive`, `worker-broadcast`, `worker-indexer`, and `worker-maintenance` roles use durable claims/checkpoints. `all-in-one` preserves panel-hosting compatibility. |
| Deployment | Docker Compose provides Redis persistence, two interactive replicas, isolated workers, read-only containers, resource limits, and readiness checks. |

## Main implementation files

- `bot.py` — startup barrier, readiness lifecycle, role selection, worker launch.
- `database/db.py` — hardened clients, strict search, cursor paging, counters,
  shared shard routing, durable indexer leases.
- `database/redis_client.py` — async Redis pool and typed state primitives.
- `database/index_policy.py` — required simple, unique, and compound indexes.
- `database/shard_router.py` — local routing plus shared snapshot hydration.
- `plugins/readiness.py` — pre-dispatch readiness and cross-replica update gate.
- `plugins/telegram_retry.py` — bounded Telegram gateway policies.
- `plugins/workload.py` — distributed search admission and interactive load gate.
- `plugins/bulk_indexer.py` — MongoDB-leased resumable worker.
- `docker-compose.yml` — multi-container production topology.

The repository files above are the complete production code; they are kept as
normal source files rather than duplicated into this document.

## Required configuration

```env
OPERATIONS_DATABASE_URI=mongodb+srv://...
SERVICE_ROLE=all-in-one
```

Use `SERVICE_ROLE=all-in-one` only for a single-process hosting panel. Docker
Compose assigns the four isolated service roles itself. `REDIS_URL` is optional
for `all-in-one`; without it, temporary state is process-local and is cleared
on restart. Every split worker role requires Redis, and Docker Compose supplies
`redis://redis:6379/0` automatically.

## Upgrade sequence

1. Back up the Operations database and every movie shard.
2. Configure a dedicated `OPERATIONS_DATABASE_URI` and Redis.
3. Install the locked dependencies.
4. If startup reports missing search tokens, run:

   ```bash
   python tools/migrate_search_tokens.py --apply
   ```

   The same resumable scan adds `language_tags` and rebuilds language counters.
5. If startup reports an empty registry, run:

   ```bash
   python tools/migrate_registry.py
   ```

6. Start the services with `docker compose up -d --build` or use
   `SERVICE_ROLE=all-in-one` on a single-process panel.
7. Run the read-only latency harness against staging before sizing production:

   ```bash
   python tools/load_test_search.py --requests 100000 --concurrency 500
   ```

## Validation boundary

The implementation removes known process-local coordination and database scan
bottlenecks, but 100,000 active users and a p95 below 200 ms are capacity
targets, not guarantees from source code alone. They must be confirmed against
the selected Redis tier, MongoDB tiers/data distribution, network region,
Telegram rate limits, query mix, and replica count using the included harness
plus an end-to-end Telegram workload.
