# MCCxBot — Long-Term Architecture Proposal

> Historical snapshot: this proposal predates the August 2026 stability and
> UI rebuild. It remains useful for future scaling choices, but does not
> describe the current implementation.

Status: proposal, not implemented. Written after Sections 1-4 of the audit
fix-up (data-loss bugs, group whitelist/auto-delete enforcement, dead-code
cleanup, TMDB removed from search results, verification gates unified) were
applied directly to the codebase. Everything below is scoped for *after*
that work, against the codebase as it stands today.

Each of the four items here was explicitly flagged "propose or implement
where feasible." None of them are feasible to implement blind, in one pass,
without the maintainer making an infrastructure/budget call first — they
involve new paid services, migrating live data, or changing the deploy
model. So this is the concrete plan to hand to whoever picks it up, not a
restatement of the problem.

---

## 1. Full-Text Search (replace regex scans)

**Problem today:** `get_search_results()` and `admin_search_files()` in
`database/db.py` build case-insensitive (`$options: "i"`) regex filters.
MongoDB cannot use a B-tree index prefix for a case-insensitive regex
regardless of anchoring, so every search is a full collection scan, fanned
across up to 5 clusters via `asyncio.gather`. Fine today because the
working set fits in each cluster's RAM; this is the first thing that
degrades as the library grows.

**Recommendation:** MongoDB Atlas Search (Lucene-backed), not a
self-hosted Elasticsearch — it's available on the same free/shared Atlas
tier already in use, so it doesn't force the cluster-consolidation
decision (#4) to happen first.

**Plan:**
1. Add an Atlas Search index on `movies.file_name` per cluster (`analyzer:
   lucene.standard`, plus a `keyword`-analyzed sub-field for exact/prefix
   matching — the current prefix-suggestion feature needs this).
2. Add `Database.get_search_results_v2()` behind a feature flag
   (`bot_config.use_atlas_search`), using `$search` instead of `$regex`.
   Keep the existing regex path alive and switchable — don't delete it
   until parity is proven.
3. Dual-run for a week: log both result sets for a sample of real queries,
   diff them, fix ranking/tokenization gaps (Malayalam/Tamil transliterated
   titles are the likely edge case — verify Atlas Search's standard
   analyzer handles them acceptably before committing).
4. Flip the flag, monitor `missed_searches` volume for a regression, then
   delete the regex path and `compile_regex`'s LRU cache.
5. `get_prefix_suggestions()` and `admin_search_files()` migrate the same
   way, same flag.

**Effort:** M-L (3-5 days) for the index + dual-run + cutover, assuming no
surprises in analyzer behavior for the non-English titles. Add 3-5 days if
transliteration handling needs custom analyzer work.

**Depends on:** nothing. Can happen independently of #2/#3/#4.

---

## 2. Shared State Engine (Redis)

**Problem today:** Everything listed here lives in a process-local Python
object, which is *why* the `fcntl` single-instance lock in `bot.py` exists
— the moment two processes run, these silently diverge:

| State | Where | Currently |
|---|---|---|
| Search sessions (pagination, query, results) | `database/db.py` `_SearchCache` | in-process OrderedDict, 600s TTL |
| Admin multi-step input state | `plugins/state.py` `ADMIN_STATE` | in-process dict, 300s TTL |
| Per-user search cooldown | `plugins/filter.py` `USER_SEARCH_COOLDOWN` | in-process OrderedDict, 10k cap |
| `bot_config` cache | `database/db.py` `_config_cache` | in-process, 60s TTL |
| `get_db_size()` cache | `database/db.py` `_db_size_cache` | in-process, 30s TTL (added this pass) |
| TMDB response cache | `tmdb.py` `_cache` | in-process, 24h TTL (added this pass) |
| Pending broadcast preview | `plugins/broadcast.py` `_pending_broadcasts` | in-process dict |
| Recent-post cooldown | `plugins/realtime_indexer.py` `RECENT_POSTS` | in-process OrderedDict |

**Recommendation:** Redis, single instance to start (Upstash/Redis Cloud
free tier is enough at current scale — this doesn't require a paid tier
until traffic actually justifies horizontal scaling, which is the whole
point of doing this).

**Plan — migrate in this order, each independently shippable:**
1. `bot_config` cache and `get_db_size()` cache first — lowest risk, pure
   read-through caches with no session semantics, good exercise for the
   Redis client/connection-pooling setup.
2. `USER_SEARCH_COOLDOWN` — a Redis `SET key EX 2` per user is a direct
   swap, no data-shape change.
3. `_SearchCache` (search sessions) — highest value, since this is what
   actually blocks running >1 process. Serialize session dicts as JSON;
   watch out for the `results` list (Mongo documents with `ObjectId` —
   needs a custom encoder or pre-stringify `_id` before caching).
4. `ADMIN_STATE` — low traffic (admin-only), but do it for correctness
   once the others are done; an admin mid-flow shouldn't break if the
   process restarts or a second instance picks up their next message.
5. `RECENT_POSTS` / `_pending_broadcasts` — low priority, low risk, do
   last or leave in-process indefinitely (broadcast state in particular is
   already documented as "doesn't survive a restart," a known limitation,
   not a bug).
6. Once 1-4 are done, drop the `fcntl` lock and the Unix-only assumption
   that comes with it (also fixes the Windows-portability gap noted in
   `BOT_BLUEPRINT.md`).

**Effort:** L (1.5-2 weeks) for all of 1-5 plus testing multi-process
behavior didn't regress anything. This is the one item on this list that
actually unlocks #3 — everything else is optimization within a
single-process ceiling until this ships.

**Depends on:** nothing technically, but do this before #3 (webhooks)
since #3 is pointless without it.

---

## 3. Stateless Workers Behind Webhooks

**Problem today:** Single process, long-polling MTProto, `fcntl`-locked to
exactly one instance. Can't scale horizontally, and a process crash means
full downtime until whatever supervises it (systemd, Docker restart
policy, etc.) brings it back.

**Recommendation:** Don't start this until #2 (Redis) has shipped — a
webhook-based multi-worker deploy with process-local state is strictly
worse than what exists today (every worker would have its own
`_SearchCache`, and a user's pagination tap could land on a different
worker with no memory of their session).

**Plan (once #2 is done):**
1. Swap `Client(...).run()`'s long-polling loop for Pyrogram's webhook
   mode (or front it with a small FastAPI/aiohttp layer that receives
   Telegram's webhook POSTs and hands updates to the existing handler
   registration — Pyrogram doesn't have first-class webhook support, so
   this is realistically a `python-telegram-bot`-style reverse-proxy
   pattern or a custom update-injection shim; budget research time for
   this specifically, it's the least "just configure a flag" part of the
   whole plan).
2. Deploy N replicas behind a load balancer, remove the `fcntl` lock
   entirely (already gone if #2 shipped cleanly).
3. Background tasks (`run_health_monitor`, `run_cache_reaper`,
   `run_indexer`) need exactly-once semantics across replicas — either
   pin them to one designated worker (env var / leader election) or move
   them to a proper task queue (Celery/RQ/arq) per the original audit's
   Phase 10 suggestion. Don't let N replicas all run the bulk indexer for
   the same channel simultaneously.

**Effort:** XL (2-3 weeks), and genuinely the highest-uncertainty item
here because of the webhook-shim research spike in step 1.

**Depends on:** #2 (Redis) must ship first.

---

## 4. Cluster Consolidation + Web Admin Panel

**Two separate decisions bundled under one audit line item — split them:**

### 4a. Consolidate the 5 free-tier clusters

This is explicitly a **budget decision**, not an architecture decision —
say that plainly to whoever approves it. The 5-cluster manual-sharding
design in `database/db.py` (the `450` MB soft-limit check in
`save_file`/`save_files_bulk`, the `AllClustersFullError` path added this
pass) is a correct, working solution to "we don't want to pay for Atlas
yet." The moment budget allows a single M10+ (or higher) cluster, or
native MongoDB sharding, most of `db.py`'s cross-cluster fan-out
(`asyncio.gather` over `self.file_cols`, the `file_registry` cross-cluster
uniqueness table, `migrate_cluster()`) becomes unnecessary complexity.
Don't build toward this speculatively — it's a one-time migration
(`mongodump`/`mongorestore` per cluster into the new deployment, then
re-point `DATABASE_URI`) that should happen the week the budget is
approved, not before.

**Effort:** S-M (2-3 days) *execution*, whenever it's approved. Zero
effort to prepare for in code — the current design already isolates the
sharding logic in `db.py`, nothing above that layer needs to know.

### 4b. Web admin dashboard

Keep the Telegram-native admin panel — it's genuinely good for quick
actions (the two-tier Content/Users&Groups/Settings/Health structure, the
new Verification Gates and Known Issues consolidation added this pass).
Add a web panel only for what Telegram chrome is structurally bad at:

- Bulk moderation across hundreds/thousands of groups (the current
  Group Manager screen paginates 20 at a time in a text list)
- Real analytics charts (today's `📊 Analytics` screen is `█`/`░`
  bar-strings in a Telegram message)
- Reviewing an `/update` commit diff before approving it (the self-updater
  fixed this pass now requires a SHA + shows a GitHub link — a web view
  could render the actual diff inline instead of "go check GitHub")
- Searchable audit logs (today: scroll the log channel)

**Recommendation:** FastAPI + a minimal frontend (htmx or a small React
app — htmx is the lower-effort choice given this is a small ops tool, not
a product surface), reading from the same MongoDB clusters read-only
except for the specific bulk-moderation actions it needs to write.
Authenticate via the same `ADMIN_ID` list (a simple shared-secret or
Telegram-login-widget flow, not a new user system).

**Effort:** L-XL (1-3 weeks depending on scope — start with read-only
analytics + audit log view, that alone is most of the value for least
effort; bulk moderation and diff review can follow).

**Depends on:** nothing technically. Can start anytime; genuinely
independent of #1/#2/#3.

---

## Suggested sequencing

```
#1 Full-Text Search ─────────────────────────────────▶ (independent)
#4b Web Admin (read-only first) ─────────────────────▶ (independent)
#4a Cluster Consolidation ───────────────────────────▶ (whenever budget allows)

#2 Redis ──────▶ #3 Webhooks + Stateless Workers
```

Only #3 is hard-blocked on another item. Everything else can run in
parallel if there's more than one person available; if it's one person,
#1 (Full-Text Search) is the best first pick — it's the only item that
fixes a problem users will actually notice getting worse over time
(search latency as the library grows), and it doesn't require any
infrastructure decision from the maintainer first.

## What NOT to do yet

- Don't start #3 (webhooks) before #2 (Redis) — confirmed above, worth
  repeating because it's the easiest mistake to make ("webhooks sound more
  scalable, let's do that first").
- Don't consolidate clusters (#4a) speculatively before budget is actually
  approved — the current 5-cluster design isn't broken, it's a working
  cost tradeoff.
- Don't build a custom full-text index / token-array field as a stopgap
  before trying Atlas Search — the original audit floated this as a
  fallback ("maintain a precomputed lowercase token array... falling back
  to regex only for the fuzzy tail"), but it's strictly more custom code
  to maintain than just using Atlas Search, which is already sitting on
  the same infrastructure this bot already pays for (or uses free-tier).
  Only reach for the custom-index fallback if Atlas Search's analyzer
  genuinely can't handle the transliterated-title edge cases after real
  testing (step 3 of #1's plan) — don't pre-emptively assume it can't.
