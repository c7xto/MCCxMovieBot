# MCCxMovieBot — Technical Architecture & Vulnerability Audit

**Scope:** Full repository (`bot.py`, `database/db.py`, `plugins/*.py`, `utils.py`, `tmdb.py`) — 6,462 lines of Python across 21 files.
**Stack:** Pyrogram 2.0.106, Motor/PyMongo (async MongoDB, up to 5 sharded Atlas free-tier clusters), aiohttp, python-dotenv.
**Scale context (per README):** production bot serving 1.5M+ indexed files across 5 MongoDB clusters — this matters a lot for the performance findings below, since several issues that look cosmetic at 1,000 documents become severe at 1.5M.

All findings below were verified by reading the actual source (not inferred) and, where the bug depends on framework internals, by downloading and reading the installed Pyrogram 2.0.106 wheel's `dispatcher.py` and `client.py` directly.

---

## Phase 1 — Architecture & Structural Mapping

### Footprint

```
bot.py                  entry point — Client subclass, single-instance lock, boot sequence
tmdb.py                 TMDB API wrapper (aiohttp)
utils.py                FSub (force-subscribe) gate logic
database/db.py          929-line Motor/MongoDB data-access layer (single God-object: Database)
database/__init__.py    empty
plugins/                17 files, auto-loaded by Pyrogram — admin.py, filter.py, start.py,
                         index.py, indexer.py, file_manager.py, group_manager.py,
                         group_connect.py, req_fsub.py, request.py, broadcast.py,
                         welcome.py, updater.py, health_monitor.py, state.py, utils.py
requirements.txt        pyrogram, TgCrypto, motor, pymongo, dnspython, python-dotenv, aiohttp
```

Configuration is split across two layers: `.env` (bot token, API credentials, Mongo URIs, `ADMIN_ID`, TMDB key — read once via `python-dotenv`) and a MongoDB singleton document `bot_config` (everything else: FSub channels, welcome text, caption template, maintenance mode, auto-delete time). `db.sync_config()` migrates `.env` values into Mongo on first boot so the admin panel can change them live without a restart — a genuinely good design choice.

One notable inconsistency worth flagging up front because it recurs throughout the report: **`utils.py` and `plugins/utils.py` are byte-for-byte identical files** (verified with `diff`). Only the root `utils.py` is actually imported anywhere (`from utils import ...`); `plugins/utils.py` is dead weight that Pyrogram's plugin loader will still import and scan for handlers (it defines none, so harmless, but it's maintenance risk — a future edit to one copy and not the other will silently diverge).

### Entry Point & Boot Flow

`bot.py` acquires an exclusive `flock` on `/tmp/mccxbot.lock` before doing anything else, so a second instance exits immediately instead of double-processing updates — correct and important for a bot that also self-restarts (see the updater section). `AutoFilterBot(Client)` is constructed with `plugins=dict(root="plugins")`, `sleep_threshold=60`, and `max_concurrent_transmissions=3`. On `start()`, it pings every configured Mongo cluster, migrates `.env` config into Mongo, clears stale indexer tasks and stale search-cache sessions, calls `ensure_indexes()`, and finally spawns `run_health_monitor()` as a background `asyncio.create_task`.

Plugin loading is entirely implicit: Pyrogram's `Client.load_plugins()` (confirmed by reading the installed wheel) does `sorted(Path("plugins").rglob("*.py"))` and imports each module in **alphabetical filename order**, registering every decorated handler it finds via `vars(module)`. This ordering is significant — see Phase 2, Bug #2, where it directly causes a broken command.

### Data Model (MongoDB)

Each configured `DATABASE_URI_N` becomes its own database (`MCCxBot_Cluster_{N}`), and each cluster gets its own `movies` collection (`file_cols[i]`). This is a manual sharding strategy built around MongoDB Atlas's 512MB free-tier ceiling — `save_file`/`save_files_bulk` check `get_db_size()` before writing and roll over to the next cluster once a cluster exceeds 450MB.

| Collection | Location | Shape | Indexes |
|---|---|---|---|
| `movies` | every cluster | `{_id, file_id, file_name, file_size, mime_type}` | `file_name` (ascending, non-unique) only |
| `users` | cluster 1 (`main_db`) | `{_id: user_id, first_name, joined, req_fsub_last}` | default `_id` only |
| `banned_users` | cluster 1 | `{_id: user_id}` | default `_id` only |
| `connected_groups` | cluster 1 | `{_id: group_id, title, added, whitelisted, banned, search_count, settings}` | default `_id` only |
| `bot_config` | cluster 1 | singleton doc, all runtime settings | default `_id` only |
| `missed_searches` | cluster 1 | `{_id: cleaned_query, count, last_searched, last_alerted, original}` | `count` (desc) |
| `pending_requests` | cluster 1 | `{user_id, movie_name, original_name, timestamp}` | none |
| `search_cache` | cluster 2 (or cluster 1 if only one configured) | `{_id: session_id, results[], query, tmdb, speed, time, auto_delete_time, ...}` | none |
| `indexer_tasks` | cluster 2 (or cluster 1) | `{_id: chat_id, state, updated}` | none |
| `settings` | cluster 1 | index-progress checkpoints | none |
| `duplicate_scan_results` | cluster 1 | cached admin dupe-scan output | none |

The most consequential fact in this table, expanded on in Phase 3, is that **`file_id` — the field used to detect duplicate uploads and to look up files for deletion by ID — has no index at all**, while `file_name` — the field that's queried with unanchored, case-insensitive regex on every single user search — has only a plain B-tree index that MongoDB cannot actually use for that query shape.

---

## Phase 2 — Deep-Dive Vulnerability & Bug Hunting

### How failures actually manifest in this codebase (important context)

Before listing bugs: I confirmed by reading Pyrogram's `dispatcher.py` that an unhandled exception inside a handler does **not** crash the bot process. `handler_worker()` wraps every handler call in `try/except Exception: log.exception(e)` and then `break`s out of that handler group, moving on. So every bug below produces a **silent failure** — the exception is written to the log, the user sees nothing happen (a spinner that never resolves, a button that does nothing), and the process keeps running. This is arguably worse operationally than a crash, because nothing alerts the admin unless they're actively tailing logs.

### Critical Bug #1 — The "✅ Done — Let Me In" FSub verification button is completely broken

`plugins/filter.py:566-568`, inside `check_fsub_callback`:

```python
async def check_fsub_callback(client: Client, callback: CallbackQuery):
    from utils import _parse_entry, _check_one_channel, _get_join_link
```

`utils.py` defines `_parse_fsub_entry`, `is_subscribed`, `is_subscribed_join_only`, and `send_fsub_message` — **`_parse_entry`, `_check_one_channel`, and `_get_join_link` do not exist anywhere in the repository** (confirmed via repo-wide grep). Every single tap of the "✅ Done — Let Me In" button — the button shown to every user who hits the force-subscribe gate, which is the bot's primary growth/retention mechanism — raises `ImportError` and silently does nothing. Users who complete the join flow correctly get permanently stuck at the FSub prompt with no recovery path short of re-triggering a fresh search or file tap (which regenerates a working prompt via `send_fsub_message`, but the "I've joined, unlock now" affordance itself never works). Given how central FSub is to this bot's monetization/growth model, this is the single highest-priority fix in the report.

### Critical Bug #2 — `/request <movie>` is silently swallowed by the search handler

`plugins/filter.py`'s catch-all search handler:

```python
@Client.on_message(
    filters.text & filters.private &
    ~filters.command(["start", "help", "about", "admin", "broadcast", "ban", "unban", "purge_cams", "reset_db", "update"])
)
async def auto_filter(client: Client, message: Message, manual_query=None):
```

`"request"` is not in that exclusion list. Because Pyrogram loads plugin files in alphabetical order (`filter.py` < `request.py`), `auto_filter` registers **before** `request.py`'s dedicated `@Client.on_message(filters.command("request"))` handler, and both live in the same default handler group (group 0). Per Pyrogram's dispatcher, the first matching handler in a group "wins" and stops further handlers in that group from running. When a user types `/request Oppenheimer`, `auto_filter` matches first (a command is still `filters.text`, and `/request` isn't excluded), treats the entire string as a search query, and `request_cmd` never fires. The typed slash command is completely dead; only the inline "📝 Request This Movie" **button** (which fires a `callback_query`, a separate dispatch path) still works. This was confirmed against the actual installed Pyrogram source, not just inferred from filter logic.

### Critical Bug #3 — "Delete ALL Duplicates" always fails

`plugins/file_manager.py:305`:

```python
deleted = await db.delete_duplicates_all()
```

`Database` in `database/db.py` has no `delete_duplicates_all` method — it defines `find_duplicate_files()` (the scan) but the bulk-delete-all counterpart was apparently never written. Every tap of "💣 Delete ALL Duplicates (Keep Oldest)" raises `AttributeError`, caught by the surrounding `try/except` in `fm_delete_all_dupes`, and shows "❌ Purge failed". The per-group delete button (`fm_del_dupes`, which calls the real `db.delete_file_by_obj_id` in a loop) works fine — only the "delete everything at once" shortcut is broken.

### Critical Bug #4 — Generic `/start` deep-link searches crash

`plugins/start.py:197`:

```python
return await route_menu(client, status_msg, session_id, "ALL", "ALL", 0)
```

`route_menu` is an alias for `filter.py`'s `show_results(client, message, session_id, page)` — a 4-parameter function. This call passes 6 positional arguments, so it raises `TypeError: show_results() takes 4 positional arguments but 6 were given` every time. This path triggers whenever a `/start <payload>` deep link doesn't start with `search_`, `file_`, or `req_` (i.e., any bare/legacy payload) and the query actually returns results — the user gets nothing back after "🔍 Searching databases...". The `search_`-prefixed and `file_`-prefixed deep-link paths (used everywhere else in the bot, e.g. request-fulfillment notifications) are unaffected since they take a different, correct branch.

### High-severity Bug #5 — Multi-admin support is a landmine

`broadcast.py` parses `ADMIN_ID` as a comma-separated list:

```python
ADMIN_ID = [int(x.strip()) for x in os.getenv("ADMIN_ID", "0").split(",") if x.strip()]
```

But `admin.py`, `file_manager.py`, `filter.py`, `group_manager.py`, `index.py`, and `updater.py` all do:

```python
ADMIN_ID = int(os.getenv("ADMIN_ID", 0))
```

Both forms run at **module import time**, which is plugin-load time, which is bot-startup time. If an operator ever sets `ADMIN_ID=123456,789012` — a format `broadcast.py`'s own code implies is supported — the bot will crash on boot with `ValueError: invalid literal for int()` the moment any of those six other modules import, before the bot even connects to Telegram. Right now the README's setup instructions only describe a single admin ID, so this landmine is currently dormant, but it means "add a second admin" is not actually a safe supported operation despite `broadcast.py` suggesting otherwise.

### Concurrency & Blocking I/O

The codebase is generally clean here: no bare `except:` blocks anywhere (grep-verified), no synchronous `requests` calls, no stray `time.sleep()` inside handlers. The one exception is `plugins/updater.py`'s `_do_update()`, which does synchronous `open(path, "wb").write()` and `os.makedirs()` for every file pulled from GitHub, inside an `async def`, with no `asyncio.to_thread`/executor offload. Because this runs as a detached `asyncio.create_task`, it doesn't block the *caller*, but it does block the **entire event loop** for its duration — meaning every live search, callback, and indexing job across all users freezes for as long as the update download+write takes. It's admin-triggered and infrequent, so the blast radius is small, but on a shared VPS with a slow disk this could visibly stall the bot for tens of seconds.

**Zero uses of `asyncio.gather` exist anywhere in the codebase.** Every single multi-cluster operation in `database/db.py` — including `get_search_results`, the function that answers every user's movie search — is a plain sequential Python `for col in self.file_cols:` loop that awaits each of the (up to 5) independent MongoDB clusters one after another. This is covered in depth in Phase 3, but it belongs here too: it's a concurrency-model choice, not just a query-tuning issue.

### Telegram API Edge Cases (FloodWait, blocked users)

`sleep_threshold=60` on the `Client` constructor is a strong baseline — Pyrogram will transparently sleep-and-retry any RPC call whose `FloodWait` is under 60 seconds, without the application code needing to do anything. `indexer.py` (channel history reads) and `broadcast.py` (mass-send loops) both add **explicit** `FloodWait` handling on top of that, correctly, because bulk operations can plausibly exceed 60s. `admin.py`, `file_manager.py`, `group_manager.py`, `req_fsub.py`, `updater.py`, `start.py`, `welcome.py`, and `request.py` have no explicit `FloodWait` handling, which is fine for one-off admin actions but is a real gap in two specific loops that iterate over an admin-configured, unbounded list of channels: `fsub_refresh_links` (loops `export_chat_invite_link` over every FSub channel) and `check_all_channels`/`channel_health_check` (loops `get_chat` + `get_chat_member` over every DB + FSub channel). At small channel counts this is invisible; if an operator configures many FSub/DB channels these loops have no per-iteration backoff or delay at all.

Blocked-user cleanup (`InputUserDeactivated`/`UserIsBlocked` → `db.delete_user`) is implemented correctly, but **only inside `broadcast.py`**. Elsewhere — the new-user log alert in `start.py`, the request-fulfillment notification in `index.py`, the admin ticket notification in `request.py` — a blocked user is caught by a generic `except Exception` and silently ignored rather than being removed from `users_col`. That means `users_col` (and therefore every broadcast's recipient count and every `get_all_users()` scan) slowly accumulates dead users between broadcasts. Not urgent, but it's an easy consistency win to centralize this into one helper.

`indexer.py`'s channel-history loop treats *any* non-`FloodWait` exception during `client.get_messages()` as fatal: it calls `db.clear_index_task()` and returns, abandoning the entire indexing run rather than skipping the bad batch and continuing. Given that indexing runs can take hours on a 1.5M-file backlog, one transient network blip forces a full manual restart (progress is checkpointed via `set_index_progress`, so no work is lost, but the admin has to notice and manually resume).

### State Management & Race Conditions

`ADMIN_STATE` (in `state.py`) is a single in-process dict keyed by `admin_id` with a 5-minute TTL — correctly isolated per-admin even under the multi-admin scenario, no collision risk there. `_pending_broadcasts` in `broadcast.py` is keyed by the admin's own chat ID — also correctly isolated.

`_cached_dupes` in `file_manager.py`, however, is a **single unkeyed global list**, not per-admin:

```python
_cached_dupes = []
```

If two admins ever run "Find Duplicates" at overlapping times (a real possibility once Bug #5's multi-admin path is fixed), the second scan silently overwrites the first admin's in-progress results, and stale pagination/delete callbacks from the first admin's session would then operate against the second admin's data.

The most consequential race is a classic check-then-act TOCTOU in `save_file()` (the single-file path used by the real-time channel indexer):

```python
for col in self.file_cols:
    if await col.find_one({"file_id": file_id}):
        return False, "Duplicate"
...
await col.insert_one(file_doc)
```

There is no unique index on `file_id` to make this atomic. Two near-simultaneous channel posts of the same file (e.g. cross-posted to two configured DB channels) can both pass the "not found" check before either insert completes, producing a genuine duplicate document. This isn't hypothetical — the codebase has built an entire admin subsystem (`find_duplicate_files`, the duplicate-scan UI, "Delete ALL Duplicates") specifically to clean up after exactly this class of race, when a unique index on `file_id` would prevent most of it at the source and turn the rest into a cheap, well-defined `DuplicateKeyError` to catch.

### Error Handling & Credential Exposure

Error handling discipline is generally good — no bare `except:`, and secrets (`BOT_TOKEN`, `API_HASH`, `DATABASE_URI*`, `TMDB_API_KEY`) are never directly logged. Two things worth an operator's attention:

1. `database/db.py` connects to every MongoDB cluster with `tlsAllowInvalidCertificates=True`. This disables TLS certificate validation on the primary data store connection — it defeats TLS's protection against MITM on that link. This is very likely a workaround for a specific Atlas/OS certificate-bundle issue rather than an intentional security tradeoff, but as written it weakens the connection security of every credential and every byte of file/user data in transit.

2. `export_config()` excludes `{_id, log_channel, admin_id, db_channels, update_channel_id, db_channel}` from the downloadable JSON backup, but **does not exclude `fsub_channels`**, which can contain private-channel invite links (`https://t.me/+xxxx`). Those links function as bearer credentials granting channel access to whoever holds them. The exported backup file is meant to be shareable/storable, so this is a mild but real secret-leakage path.

3. `updater.py`'s self-update mechanism downloads raw file content over `aiohttp` from `raw.githubusercontent.com` for a hardcoded repo/branch, writes it straight to disk with no checksum or signature verification, and then `os.execv`'s into the new code with no backup of the previous version. It's admin-gated (`filters.user(ADMIN_ID)`), so this isn't exploitable by ordinary users, but it means a compromise of the `c7xto/mccxmoviebot` GitHub repo (or its DNS/hosting) translates directly into remote code execution on every bot instance that clicks "Update", with no verification step and no easy rollback.

---

## Phase 3 — Performance & Optimization Audits

### The core problem: search is architecturally guaranteed to be a full collection scan, at 1.5M+ documents

`get_search_results()` — the function behind every single user search — builds a query like this for a multi-word query:

```python
raw_pattern = r".*[\s\.\+\-_]".join(words)
regex = compile_regex(raw_pattern)          # re.IGNORECASE
filter_mongo = {"file_name": regex}
cursor = col.find(filter_mongo).sort("$natural", -1).skip(offset).limit(limit)
```

Two independent things here each force a full collection scan, and together they guarantee one:

- The regex is **not anchored at the start** (`^`) and is **case-insensitive**. MongoDB can only use a B-tree index to accelerate a regex if the pattern is left-anchored *and* the query uses a case-sensitive match (or a matching case-insensitive collation is defined on the index itself, which this index does not have). Since almost every real query here is an infix/case-insensitive match, the existing `file_name` index (`ensure_indexes()` only creates a plain ascending index) is essentially decorative for this workload.
- `.sort("$natural", -1)` sorts by reverse insertion order, which itself requires MongoDB to scan the collection from the end rather than use an index-ordered scan.

The practical result: at README-stated scale (1.5M+ files, sharded across 5 clusters), **every user search performs a full collection scan on up to 5 separate multi-hundred-thousand-document collections**, sequentially, and this is currently the single biggest latency and infrastructure-cost risk in the codebase. The fix isn't a tuning tweak — it needs either a MongoDB **text index** (`$text`) with proper tokenization, or (better, given the "words in order, flexible separators" matching behavior the bot deliberately wants) a maintained normalized/tokenized field with a supporting index, or a move to a dedicated search layer (Meilisearch/Typesense/Atlas Search) if result relevance and fuzzy matching are also a priority — see Phase 4.

### Sequential multi-cluster fanout, everywhere

Every multi-cluster method in `database/db.py` — `get_search_results`, `admin_search_files`, `get_bad_files`, `get_prefix_suggestions`, `get_total_files`, `get_bot_stats`, `get_files_by_language`, `count_by_pattern`, `purge_by_pattern`, `save_file`'s per-cluster duplicate check — loops over `self.file_cols`/`self.dbs` with a plain `for` loop and `await`s each cluster in turn:

```python
for col in self.file_cols:
    cursor = col.find(filter_mongo)...
    async for doc in cursor:
        ...
    if len(files) >= limit:
        break
```

Not one of these uses `asyncio.gather`. Since these are up to 5 independent MongoDB connections (plausibly separate Atlas projects/regions, per the free-tier-per-cluster design), sequential execution means user-facing search latency scales roughly linearly with cluster count instead of being bounded by the slowest single cluster — i.e., today's search is up to ~5x slower than it needs to be purely from this one structural choice, independent of the indexing problem above. This is the highest-leverage, lowest-risk optimization in the whole codebase: converting these loops to `asyncio.gather(*[col.find(...) for col in self.file_cols])`-style fan-out is a mechanical, low-risk change with an immediate, measurable latency win on the bot's most-used feature.

### Missing indexes that directly cause N+1-style scans

- **`file_id` has no index anywhere.** `save_files_bulk`'s duplicate check (`col.find({"file_id": {"$in": incoming_ids}})`), `save_file`'s per-item check, and `delete_file_by_id` all filter on `file_id` without index support — every bulk-index batch during channel indexing does a full scan per cluster per batch. A unique index on `file_id` per cluster would both fix the TOCTOU race noted in Phase 2 and turn these into O(log n) lookups.
- `pending_requests` has no index, and `find_matching_requests` regex-searches `movie_name` on every newly indexed file — low volume today, but it's an unindexed scan on the hot "new file just landed" path.
- `connected_groups.search_count` has no index; `get_top_groups()` does an unindexed sort — low severity given group counts are small, but free to fix alongside the others.

### Redundant round-trips that don't need to hit the database at all

`db.get_search()`/`db.save_search()` route every pagination click and every "send this file" tap through a MongoDB round-trip to the `search_cache` collection, even though sessions are short-lived (auto-delete at 5-10 minutes) and the bot enforces single-instance execution via the `flock` lock anyway — there's no multi-process reason this needs to be externalized to Mongo. An in-process TTL cache (e.g. `cachetools.TTLCache`) would eliminate a network round-trip on every single button tap in the bot with no loss of correctness, at the cost of losing in-flight sessions across a restart — which the auto-delete window already implies is an acceptable tradeoff (search sessions aren't meant to survive long anyway).

Positive pattern worth calling out: `db.get_config()` already does exactly this kind of caching correctly — a 60-second in-process TTL cache backing the `bot_config` singleton doc, invalidated on every `update_config()` call. This is good design and should be the template for the search-session cache above, not something that needs revisiting.

### Memory & in-process state hygiene

`USER_SEARCH_COOLDOWN` and `RECENT_POSTS` are both bounded, but crudely — both do a full `.clear()` once they hit their cap (10,000 and 1,000 entries respectively) rather than evicting the oldest entries. For `RECENT_POSTS` specifically (the update-channel repost-cooldown cache), this means a large indexing burst can blow past the 1,000-entry cap, wipe the entire cooldown cache, and cause a cluster of previously-deduplicated titles to be reposted to the update channel. A proper LRU (`collections.OrderedDict` with `move_to_end`/`popitem(last=False)`, or `functools.lru_cache`-style eviction) would fix this cheaply.

`MISSED_CACHE = set()` at the top of `filter.py` is declared and never referenced anywhere else in the file or the codebase (grep-confirmed) — dead code left over from an earlier implementation of missed-search deduplication (the actual logic now lives correctly in `db.log_missed_search`'s cooldown field). Harmless, but worth deleting during any cleanup pass.

---

## Phase 4 — Feature Expansion & Optimization Blueprint

### Critical Fixes (do these first — each is a currently-broken, user-facing feature)

1. **Fix the FSub "Done" button** (Bug #1) — replace the broken `from utils import _parse_entry, _check_one_channel, _get_join_link` import in `filter.py`'s `check_fsub_callback` with the real helpers that already exist (`_parse_fsub_entry`, and inline logic equivalent to what `send_fsub_message` already does), or better, refactor the link-resolution/membership-check logic that's currently duplicated across `utils.py`, `req_fsub.py`, and `filter.py` into one shared module so this class of drift can't happen again.
2. **Fix `/request`** (Bug #2) — add `"request"` (and, for defense in depth, `"filesearch"`, `"stats"`, `"cancel"`, `"reset_index_progress"`, `"confirm_reset"`) to `auto_filter`'s command exclusion list in `filter.py`. Better long-term fix: give `auto_filter` a higher numeric group than command handlers, or centralize the "is this text actually a search query" decision behind one explicit allowlist rather than a hand-maintained per-file exclusion list that's already out of sync in three separate files (`filter.py`, `file_manager.py`, `group_manager.py` all maintain their own near-duplicate exclusion lists for their respective catch-all input handlers).
3. **Implement `db.delete_duplicates_all()`** (Bug #3) — it's a small, mechanical addition (iterate `find_duplicate_files()` output, `delete_file_by_obj_id` everything except the first ID per group) and the UI already calls it correctly.
4. **Fix the `route_menu` call signature** (Bug #4) in `start.py` — drop the two stray `"ALL", "ALL"` arguments so it matches `show_results(client, message, session_id, page)`.
5. **Unify `ADMIN_ID` parsing** (Bug #5) — pick one representation (a list is the right one, since `broadcast.py` already assumes multi-admin) and import it from a single shared module everywhere, the same way `plugins/state.py` centralizes admin state today.
6. **Add a unique index on `file_id`** per cluster (`col.create_index("file_id", unique=True)` in `ensure_indexes()`), and catch `DuplicateKeyError` at the insert sites — this closes the TOCTOU race and gives the existing duplicate-cleanup tooling a lot less to clean up in the first place.

### Optimization Wins (no behavior change, meaningfully faster/cheaper)

1. **Parallelize every multi-cluster fanout with `asyncio.gather`.** This is the single highest-impact change in the report relative to effort — it's a mechanical rewrite of well-isolated loops in `database/db.py`, and it directly cuts perceived search latency by up to ~5x on the bot's core feature.
2. **Fix the search index problem.** At minimum, add a MongoDB text index on `file_name` and rewrite `get_search_results`/`get_bad_files`/`admin_search_files` to use `$text` with a text score sort instead of unanchored regex + `$natural`. If result ranking/fuzzy matching quality also needs to improve (see feature ideas below), this is also the natural point to evaluate Atlas Search or an external index (Meilisearch/Typesense) instead of MongoDB regex entirely.
3. **Move `search_cache` to an in-process TTL cache** instead of a MongoDB round-trip on every pagination click and file-send tap, following the same pattern already used successfully for `bot_config`.
4. **Replace the clear-everything eviction** in `USER_SEARCH_COOLDOWN`/`RECENT_POSTS` with real LRU eviction so a traffic/indexing burst can't reset unrelated users' cooldowns or cause repost bursts.
5. **Batch the admin analytics queries** in `get_files_by_language`/`get_bot_stats` (currently `languages × clusters` sequential `count_documents` calls) into a single `$facet` aggregation pipeline per cluster, run in parallel across clusters.

### High-Value Feature Enhancements

Grounded specifically in what this codebase already has partially built or clearly gestures toward:

- **Proper fuzzy/typo-tolerant search.** The regex-based matcher already has ad hoc infrastructure for this (`get_prefix_suggestions`'s "did you mean" feature, `find_duplicate_files`'s name-normalization logic for near-duplicate detection). Both of those are evidence the team already wants fuzzy matching — formalizing it with a trigram/n-gram index or a real search engine would upgrade "did you mean" from a crude prefix-match into genuine fuzzy search, and would let `find_duplicate_files` reuse the same scoring instead of maintaining its own separate normalization regex.
- **A real request-fulfillment queue.** `pending_requests` + `_fulfill_matching_requests` already implements "notify user when their request is indexed," matched via a fairly blunt 5-char-prefix regex OR-query. Since this is already an event-driven pipeline (triggered from `index_new_files`), it's a natural place to add duplicate-notification suppression (two files matching the same request in quick succession currently can both fire before the first `delete_pending_request` completes — a small race noted in Phase 2) and an admin-facing request-backlog view with age/priority sorting, beyond the existing "Top Missing Files" report.
- **Streamlining the inline/group search UX.** `group_connect.py` already reimplements most of `filter.py`'s pagination/caption/button logic in parallel (`_build_group_buttons`, `_build_caption` duplicate `filter.py`'s `show_results` almost line-for-line). Consolidating these into one shared result-renderer parameterized by "deep-link vs. inline button" would reduce the surface area where a fix in one search path (like the ones in this report) has to be remembered and re-applied in the other — this is exactly the kind of drift that produced Bug #1.
- **Telegram inline mode.** Given the bot already has a fast lookup path (`get_file`, cached search sessions) and a mature caption/button builder, exposing search via `@botname query` inline mode (rather than requiring a DM or group message) is a natural, high-visibility feature extension that reuses almost all existing plumbing.

---

## Summary

The codebase shows real engineering maturity in places — the multi-cluster storage-quota rollover, the live-reloadable Mongo-backed config with a correctly-invalidated cache, the FloodWait-aware bulk indexer/broadcaster, and the deliberate single-instance lock are all sound design choices, and there are no bare excepts, no blocking HTTP calls, and no obvious credential logging. But there are four handler-crashing bugs currently live in production (FSub verification, `/request`, bulk duplicate delete, generic deep-link search) that are each silent — no crash, no alert, just a stuck UI — which is precisely the failure mode that's easiest to ship and hardest to notice without dedicated log monitoring. And the core search path, the feature the entire bot exists to serve, is structurally guaranteed to full-scan multiple 300K+ document collections on every query because of an unanchored case-insensitive regex plus a `$natural` sort, with zero use of concurrent fan-out across the very clusters that were sharded specifically to handle this scale. Fixing the six critical/high items and parallelizing the cluster fanout would likely be the two highest-leverage afternoons of work available on this codebase.
