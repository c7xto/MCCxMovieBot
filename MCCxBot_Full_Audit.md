# MCCxBot — Full Architecture, Code Quality & Product Audit

Every `.py` file (bot.py, utils.py, tmdb.py, database/db.py, all 15 files in
plugins/, both tools/ scripts) plus README.md, BOT_FEATURES.md and
BOT_BLUEPRINT.md were read in full before writing this. Line numbers below
refer to the current state of the files on disk.

**Top-line verdict:** this is not a typical "vibe-coded" Telegram bot. The
codebase already shows real engineering discipline — a centralized
`file_registry` to fix a genuine cross-cluster duplicate bug, a shared
crash-callback convention for background tasks, a config cache with proper
invalidation, precompiled regex constants, an existing internal blueprint
document. Whoever wrote `BOT_BLUEPRINT.md` already anticipated a lot of the
"obvious" critique. So this audit does not re-litigate what's already
documented as intentional (fail-open FSub checks, Unix-only `fcntl` lock,
etc.) — it focuses on what the blueprint *doesn't* say: three concrete
silent-failure bugs in the storage layer, a duplicated-code inventory, a
regex/TMDB performance problem that adds real user-facing latency, a
security concern in the self-updater, and a product redesign for what this
bot needs to become to serve a much larger audience.

---

## PHASE 1 — Project Understanding

### Folder structure

```
bot.py                  Entry point + startup sequence
utils.py                 ADMIN_ID, main FSub check, FSub prompt message
tmdb.py                 TMDB "search/multi" lookup (no caching)
database/db.py           1241-line single Database class — all persistence
database/__init__.py     empty
tools/migrate_registry.py        one-time file_registry backfill
tools/verify_db_performance.py   offline DB-layer test suite
plugins/
  admin.py (1362)        /admin root + Content/Settings/Health submenus
  filter.py (796)         PM search + file delivery (the core handler)
  file_manager.py (634)   duplicate/bulk-delete/migrate/rename tools
  group_manager.py (412)  group ban/whitelist/settings admin screens
  index.py (392)          bulk "Super-Indexer" (forward → index a channel)
  group_connect.py (376)  group search + bot-added-to-group welcome
  req_fsub.py (397)       Request-FSub + Two-Stage Verification gates
  start.py (365)          /start, deep-link router, home panel
  updater.py (214)        GitHub self-updater (os.execv restart)
  broadcast.py (223)      /broadcast to users/groups, schedule, preview
  indexer.py (287)        real-time indexer + rate-limited auto-poster
  health_monitor.py (178) cluster ping loop, cache reaper, crash logger
  welcome.py (86)         group welcome via chat_member_updated
  request.py (102)        /request ticket + manual "mark uploaded"
  state.py (30)           shared ADMIN_STATE dict, 5-min TTL
```

### Architecture

Single Python process, one Pyrogram `Client` subclass (`AutoFilterBot`),
long-polling MTProto (not webhook-based), plugin auto-loading via
`plugins=dict(root="plugins")` — every file under `plugins/` is imported at
startup and every `@Client.on_message` / `@Client.on_callback_query`
decorator in it self-registers. There is no manual router, no dependency
injection, no web server. All persistence goes through one `Database`
object (`database/db.py`), imported everywhere as `from database.db import
db` — effectively a global singleton, not something dependency-injected
into handlers.

### Startup sequence (bot.py)

1. `_verify_environment()` — Python 3.14 asyncio loop patch, then a
   certifi CA-bundle sanity check. Runs **before** `import pyrogram` occurs
   anywhere, `sys.exit(1)` on failure.
2. Single-instance lock via `fcntl.flock` on `/tmp/mccxbot.lock` (Unix-only
   — already flagged in BOT_BLUEPRINT.md §7 as a Windows-portability gap).
3. `Client.start()` → `get_me()` → ping every configured Mongo cluster →
   `db.sync_config()` (one-time `.env` → Mongo field migration) → clear
   stale indexer tasks → clear old search sessions → `ensure_indexes()` →
   launch `run_health_monitor` and `run_cache_reaper` as tracked background
   tasks (`task.add_done_callback(_log_task_crash)`).

### Handler flow (PM search, the core path)

`auto_filter` (filter.py) is a catch-all text handler gated by a hardcoded
command-exclusion list. Ban check → maintenance-mode check → main FSub
check → per-user 2s cooldown → `clean_query()` strips filler words →
`db.get_search_results()` fans a regex query across every configured
cluster via `asyncio.gather` → TMDB enrichment (sequential, see Phase 5) →
session saved into an in-process `_SearchCache` → `show_results()` renders
page 0. Every subsequent tap (pagination, sort, series-expand, Data Saver,
send-file) is a callback query that re-reads the same cached session by
`session_id` and re-renders through one shared `_render_results_view()`.
File delivery itself is gated again by up to three independently
toggleable layers: main FSub → Request-FSub → Two-Stage Verification (see
Phase 3 for why stacking these matters).

### Database flow

Manual, application-level sharding across up to 5 independent MongoDB
Atlas clusters (`DATABASE_URI`…`DATABASE_URI_5`), each capped at a 450MB
soft limit (512MB hard free-tier cap). `movies` collections are per-cluster
and hold the searchable file index; `users`, `banned_users`, `bot_config`,
`connected_groups`, `missed_searches`, `pending_requests`, `file_registry`
all live only on cluster 1 (`main_db`); `indexer_tasks` lives on cluster 2.
`file_registry` is the single cross-cluster uniqueness guarantee for
`file_id` (a real bug fix — per-cluster unique indexes alone can't stop the
same file landing in two different clusters). Every search fans out to
every cluster in parallel and merges/dedupes in Python.

### Cache flow

Three in-process, single-instance-only caches, none shared across
processes (fine today since `fcntl` enforces exactly one process; a hard
blocker if this ever needs to scale past one process — see Phase 10):
- `_SearchCache` (`OrderedDict`, maxsize 2000, TTL 600s) — search-session
  pagination state.
- `_TrendingCache` (maxsize 500, 24h rolling window) — home-panel trending
  buttons.
- `_config_cache` (module global, 60s TTL) — `bot_config` document,
  invalidated explicitly on every `update_config()` call.

### File / config usage

`.env` seeds `bot_config` once via `sync_config()`; after that, `/admin` is
the actual source of truth for everything except `API_ID`/`API_HASH`/
`BOT_TOKEN`/`ADMIN_ID`/`DATABASE_URI*`, which are only ever read from the
environment (never migrated into Mongo).

### Async flow / state management

Genuinely good async hygiene overall: no blocking calls found on the hot
path (the one synchronous disk write in `updater.py` is explicitly wrapped
in `asyncio.to_thread`), every fire-and-forget task either fully
self-guards its own body or is wrapped with `_log_task_crash`. Admin
multi-step input flows share one `ADMIN_STATE` dict (`plugins/state.py`)
with a 5-minute TTL, read by three near-identical catch-all handlers
(`admin.py`, `file_manager.py`, `group_manager.py`) that each check their
own state-string prefix and `raise ContinuePropagation` if it's not theirs.

### User & admin lifecycle

**User:** `/start` → `save_user()` (silent, logs a "new user" alert to the
log channel) → home panel or deep-link dispatch → search → gate stack →
file delivered with an auto-delete timer. Blocked/deactivated users are
detected reactively (on send failure) and pruned from `users` during
broadcasts and request-fulfillment attempts, not proactively.

**Admin:** identity is entirely `ADMIN_ID` (a comma-separated env var,
parsed once in `utils.py`). The in-Mongo `admin_id` config field is
display-only and does **not** grant access — see Phase 2/6, this is a real
trap for whoever configures it expecting it to work.

---

## Dependency Map

```
bot.py
 ├─ database.db (db singleton)
 └─ plugins.health_monitor (run_health_monitor, run_cache_reaper, _log_task_crash)

database/db.py
 └─ plugins.filter.LANGUAGES   (local import, inside get_files_by_language())

utils.py
 └─ database.db

tmdb.py                (standalone — no internal deps)

plugins/state.py        (standalone — no internal deps)

plugins/filter.py
 ├─ database.db, utils (is_subscribed*, send_fsub_message, ADMIN_ID)
 ├─ plugins.req_fsub (check_and_show_req_fsub, check_and_show_two_stage)
 └─ tmdb

plugins/start.py
 ├─ plugins.filter (route_menu, auto_filter, _build_caption, _auto_delete_file)
 ├─ utils, tmdb, database.db
 └─ plugins.req_fsub (check_and_show_two_stage)

plugins/group_connect.py
 └─ database.db, plugins.filter (six helpers), plugins.health_monitor, tmdb

plugins/req_fsub.py
 └─ database.db, plugins.filter (_build_caption, _auto_delete_file — local import)

plugins/index.py            → database.db, utils.ADMIN_ID, plugins.filter.send_smart_log, plugins.health_monitor
plugins/indexer.py           → database.db, tmdb, plugins.health_monitor
plugins/admin.py             → database.db, plugins.state, utils.ADMIN_ID, plugins.health_monitor.check_all_channels
plugins/file_manager.py      → database.db, plugins.state, plugins.health_monitor, utils.ADMIN_ID
plugins/group_manager.py     → database.db, plugins.state, utils.ADMIN_ID
plugins/broadcast.py         → database.db, utils.ADMIN_ID, plugins.health_monitor
plugins/updater.py           → utils.ADMIN_ID, plugins.health_monitor
plugins/welcome.py           → database.db
plugins/request.py           → database.db
plugins/health_monitor.py    → database.db, plugins.filter.send_smart_log
```

Notable: `database/db.py` reaching *into* `plugins/filter.py` for a
constant (`LANGUAGES`) is the one inversion of the expected dependency
direction (data layer depending on a UI-layer module) — it's exactly why
`tools/verify_db_performance.py` needs a 90-line Pyrogram-isolation hack to
test the DB layer at all. `LANGUAGES`/`QUALITIES` belong in a shared
constants module both layers can import without either depending on the
other.

---

## PHASE 2 — Code Quality Review

Findings are ranked by real-world impact, not by how interesting they are
to read about.

### 🔴 Critical — silent data loss

| # | File / Line | Problem | Why it's bad | Fix |
|---|---|---|---|---|
| 1 | `database/db.py:460-501` `save_files_bulk()` | When **every** cluster's pre-check (`size >= 450`) trips, the function returns `(0, duplicates)` normally — it never raises. | `plugins/index.py`'s `run_indexer()` (lines 138-143) only detects "database full" by pattern-matching an **exception message** (`"space quota"`/`"over your space"`). Since the common case (the deliberate 450MB safety margin) never raises, the bulk indexer runs a channel to 100% completion, reports "🎉 SUPER-INDEX COMPLETE!", and silently saved **zero** files once all clusters fill — with no "add a new cluster" message, ever, for this path. Only the rarer case of Mongo itself throwing a real quota exception is reported correctly. | Make the pre-check-full case return a distinct sentinel (e.g. `(0, duplicates, all_full=True)`) or raise a dedicated `AllClustersFullError`, and have `run_indexer` treat it identically to the exception-path "Database Full" branch it already has. |
| 2 | `plugins/indexer.py:197-241` `index_new_files()` | `success, return_msg = await db.save_file(media)` — the `if log_channel and success:` and `if success:` blocks both simply do nothing when `success` is `False`. | Real-time single-file indexing has **zero failure path**. If clusters fill up during normal operation, every new upload to the DB channel vanishes from the index with no admin notification at all — worse than bug #1, because this is the higher-frequency, ongoing ingestion path (every new upload), not a one-off bulk job. | Add an `else` branch: if `return_msg == "All clusters full"`, send an alert to the log channel (reuse `send_smart_log`) so the admin finds out the moment it happens, not weeks later when a movie can't be found in search. |
| 3 | `database/db.py:690-715` `migrate_cluster()` | Copies every document from the source cluster's `movies` collection into the destination via `insert_many`, but **never deletes from the source**, and bypasses `file_registry` entirely (inserts directly instead of going through `save_file`/`save_files_bulk`, which the codebase's own blueprint explicitly warns against — BOT_BLUEPRINT.md §3). | The entire point of "Cluster Migration" (admin panel → File Manager → 📦 Cluster Migration) is to relieve a full cluster. As written, it **duplicates storage instead of moving it** — the source cluster's space is never freed (defeating the feature's purpose), and the file now physically exists in two clusters while `file_registry` still only reflects one. Search-facing results are saved from visibly duplicating (because `get_search_results` dedupes by `file_id` after merging), but the underlying storage bug means migrating *accelerates* hitting the 512MB cap on two clusters instead of relieving one. | After a successful `insert_many` into the destination, delete the same batch's `_id`s from the source collection. Route new documents through `_registry_bulk_reserve`-aware logic (or just re-tag them under a note that they're already registered) instead of a raw `insert_many`. |

### 🟠 High — orphaned / dead features (admin believes something is enforced; it isn't)

| # | File / Line | Problem | Why it's bad | Fix |
|---|---|---|---|---|
| 4 | `plugins/group_manager.py:374-408` writes `group.settings.auto_delete_time`; `plugins/group_connect.py:259` reads only `config.get("auto_delete_time", 300)` (the **global** default) | The per-group auto-delete override (advertised in BOT_FEATURES.md §8: *"Per-group settings: individual auto-delete time override"*) is fully built on the write side (admin UI, DB field, confirmation message) and **never read** anywhere. `gm_view_settings` even displays the stored value back to the admin, implying it's live. | An admin sets a group's auto-delete to e.g. 2 minutes, sees "✅ Auto-delete for group X set to 2 minute(s)", and every file in that group still deletes after the global default. Trust-breaking once discovered. | In `group_search()`, fetch `group = await db.get_group(message.chat.id)` and prefer `group.get("settings", {}).get("auto_delete_time")` over the global config value. |
| 5 | `plugins/group_manager.py:247-256` (`gm_toggle_mode`) sets `bot_config.group_whitelist_mode`; **no handler anywhere checks it** | "Whitelist Mode (only approved groups)" is a real, tappable, persisted admin toggle with zero enforcement. `group_connect.py`'s `group_search()` and `auto_connect_group()` only check `is_group_banned()` — never `is_group_whitelisted()` (which exists in `db.py:364-368` and is otherwise unused) or the whitelist mode flag at all. | Any group can use the bot regardless of whitelist state — the admin-facing promise ("only approved groups") is not backed by any code. | Add the check to `auto_connect_group()` (leave non-whitelisted groups immediately when in whitelist mode, same pattern already used for banned groups) and to `group_search()`. |
| 6 | `database/db.py` `get_config()` default dict seeds `"group_whitelist_enabled": False`, but every actual read/write in the codebase uses a **different** key, `"group_whitelist_mode"` | Config-schema drift: one field is dead weight seeded on every fresh `bot_config` document and never touched again. | Confusing for anyone reading the schema cold (BOT_BLUEPRINT.md's own schema table lists both, without flagging the mismatch). | Delete `group_whitelist_enabled` from the default dict; `group_whitelist_mode` ("whitelist"/"blacklist") is the real field. |
| 7 | `database/db.py:643-650` inside `find_duplicate_files()` | Drops and rewrites a `duplicate_scan_results` Mongo collection on every admin-triggered scan — but the actual UI (`file_manager.py`'s `_run_duplicate_scan` / `_cached_dupes`) reads the **in-memory return value** of the same function, never the Mongo collection. | Pure write-and-never-read: burns write ops and precious free-tier storage (results can include up to 100 groups × up to 10 IDs each) for a collection nothing ever queries back. | Delete the `dupes_col.drop()` / `insert_many()` block, or repurpose it as an actual audit trail if that's ever wanted (currently it's neither). |
| 8 | `database/db.py:68` `admin_id` config field (edited via admin.py's "👤 Set Admin ID") | Writing this field only ever changes what's *displayed* back in the admin panel — actual access control is 100% `ADMIN_ID` from `.env`, parsed once at import time in `utils.py`. `admin.py` line ~918 does warn about this in the confirmation text, so it's not silent, but it's still a footgun for anyone who taps it expecting to add a co-admin. | An operator could reasonably believe they just granted someone admin access and not restart the bot, leaving that person with zero actual access while believing otherwise. | Either wire it to `ADMIN_ID` at runtime (append to the in-memory list, not just Mongo) or remove the field/button entirely and document that admin changes require an env var + restart. |

### 🟡 Medium — duplicated logic / naming / maintainability

| # | File / Line | Problem | Fix |
|---|---|---|---|
| 9 | `_no_preview()` reimplemented independently in `utils.py:6-11`, `start.py:14-22`, `filter.py:17-22`, `admin.py:33-34` (imported nowhere, redefined ad hoc), `group_connect.py:14-19`, `welcome.py:8-13` | Six copies of the same 6-line `LinkPreviewOptions` compatibility shim. Move to `utils.py` (already defines one) and import it everywhere else instead of re-declaring. |
| 10 | `_html()` escape helper duplicated in `start.py:43-45`, `filter.py:55-56`, `indexer.py:23-24` (group_connect.py correctly imports it from `filter.py` — the right pattern, just not applied consistently) | Same fix: one shared `utils.py` (or a small `formatting.py`) helper, imported everywhere. |
| 11 | `plugins/index.py` (bulk "Super-Indexer") vs `plugins/indexer.py` (real-time indexer) | Two files differing by one letter, doing genuinely different things. Easy to grep the wrong one, easy for a new contributor to open the wrong file. Rename to `bulk_indexer.py` / `realtime_indexer.py`. |
| 12 | `plugins/filter.py:403-405` excludes `"about"` and `"purge_cams"` from the auto-filter catch-all — neither command exists (`purge_cams` was explicitly removed per `admin.py`'s own comment at line ~1045); `plugins/group_connect.py:153` excludes `"connect"`, which also has no handler | Harmless today (just prevents those words being treated as searches) but it's drift — a future reader will assume these commands exist somewhere. Delete stale entries when a command is removed. |
| 13 | `plugins/request.py:30-32` | `if is_callback: return await message_obj.reply_text(error_msg) else: return await message_obj.reply_text(error_msg)` — identical branches, dead conditional. Collapse to one line. |
| 14 | `plugins/request.py` manual "✅ Mark Uploaded & Notify User" (`mark_request_done`, lines 63-103) never checks whether `indexer.py`'s `_fulfill_matching_requests()` already auto-fulfilled the same request | If auto-fulfillment already notified the user and deleted the pending-request row, an admin who later taps the stale ticket button sends a **second** "your movie is ready" notification. Have `mark_request_done` check `db` for the pending request first and short-circuit with "already fulfilled" if it's gone. |
| 15 | Comment style: `# FIX #8:`, `# BUG FIX #1 + #7:`, `# DEAD END FIX #3`, `# B2:`, `# C1:`, `# C9:`/`# C10:`, `# F1:`/`F3`/`F4`/`F6`/`F7`/`F9`/`F10`, `# G1:`-`G5:`, `# A10:` scattered through `admin.py`, `file_manager.py`, `group_manager.py`, `health_monitor.py` | These read like an internal ticket tracker's IDs pasted directly into source instead of living in commit messages / an issue tracker. Harmless functionally, but meaningless to a new contributor without the original numbering scheme. Low priority cleanup. |
| 16 | `database/db.py`: `get_db_size()` (a live `dbstats` Mongo command) is called once per candidate cluster, sequentially, inside `save_file()` and `save_files_bulk()`'s cluster-selection loop | Once earlier clusters fill up, every save has to sequentially call `dbstats` on each already-full cluster before reaching one with room — added round-trip latency on every save, worse the more clusters have filled. Cache cluster sizes with a short TTL (similar pattern to `_config_cache`) instead of calling `dbstats` on every single insert/batch. |

### ✅ What's already solid (worth saying, not just what's wrong)

- No blocking synchronous I/O found on any hot path; the one real disk
  write in the self-updater is correctly wrapped in `asyncio.to_thread`.
- Every fire-and-forget background task either can't crash (fully
  try/excepted) or is wrapped with `_log_task_crash` — genuinely good
  discipline that most bots this size skip entirely.
- All long-lived in-process caches/dicts are bounded (LRU-capped or TTL'd)
  except the small, trusted-admin-only dicts, so there's no realistic
  memory-leak surface.
- `file_registry`'s reserve-then-insert-then-rollback-on-failure pattern in
  `save_file`/`save_files_bulk` is a correct, race-free design for
  cross-cluster uniqueness given MongoDB's atomic unique-index guarantee —
  this is not a naive "check then insert" race.
- Regex ReDoS is not a real risk here: every user-supplied token that
  reaches a Python-side regex goes through `re.escape()` first.

---

## PHASE 3 — User Experience Review (playing a first-time user)

Walking through cold: `/start` → video/photo welcome renders (network
round-trip for the media itself) → type "Leo" → "🔍 Searching databases..."
placeholder → wait → results. Concretely, here's where it drags or
confuses:

- **The search "flash".** When a poster is found, the code has to
  `message.delete()` the plain status message and send a **brand new**
  photo message (Telegram can't convert a text message into a photo
  message in place). The user watches the "🔍" message disappear and a new
  message pop in a beat later — a visible flicker on every first search
  with a poster. Fix: always send the initial placeholder as a photo (a
  bot logo / generic poster) and `edit_media`/`edit_caption` into the real
  poster once ready, instead of delete+resend.
- **Up to ~20 seconds of invisible dead air is possible before results
  render at all** — see Phase 5, this is the single biggest thing making
  the bot *feel* slow even though the DB search itself is fast.
- **Gate-stacking fatigue.** Main FSub, Request-FSub, and Two-Stage
  Verification are each independently reasonable, but they compose. If an
  admin turns all three on, a brand-new user can face: join Main FSub
  channel(s) → tap "Done" → search → tap a file → join Request-FSub
  channel → tap "I've Requested" → join Two-Stage Channel 1 → tap
  "Continue" → join Two-Stage Channel 2 → tap "Continue" → *finally* get
  the file. That's up to 4 separate join-and-confirm round trips for one
  file. Nothing in the admin panel warns about this cumulative effect —
  each gate's menu only talks about itself in isolation.
- **The "no results" screen leads with blame** ("something Is Wrong ❌")
  before offering anything useful. It reads like the bot is broken, not
  like "we don't have this yet, but here's what to do."
- **Too many confirmation taps for common admin actions** that are already
  low-risk and reversible (e.g. per-item duplicate deletion), while the
  one truly catastrophic action (`/reset_db`) is correctly the hardest to
  trigger (typed `/confirm_reset`) — the friction is proportional to risk,
  which is right, but the day-to-day admin taps (File Manager, Group
  Manager) could shed a menu level in places (see Phase 9).
- **English-only UI chrome for a Malayalam/Tamil/Telugu/Hindi/Kannada
  content community.** Every button, every system message, is English.
  The *content* is multilingual but the *interface* isn't — a
  language-preference toggle (persisted like Data Saver already is) is low
  effort and high goodwill here.
- **"🗣️ : {first_name}" line in the results caption** reads like a debug
  artifact / leftover template variable rather than intentional copy — it
  doesn't add information the user needs (they know their own name) and
  wastes a line of a message that's otherwise information-dense.

### Redesigned flow (fewest taps to what the user wants)

1. `/start` → home panel unchanged, but trending buttons prioritized
   above the promotional buttons (Add to Group / Join Channel), since
   trending taps convert directly into a result while promo buttons are a
   dead end for "I want a movie right now."
2. Type a query → **render results with the DB match immediately** (no
   TMDB wait); poster/synopsis/rating swap in via `edit_caption`/`edit_media`
   a moment later if TMDB responds in time. Perceived latency becomes
   "DB speed," not "DB speed + up to 2 sequential TMDB round trips."
3. Tap a file → **one combined gate screen** if more than one gate is
   outstanding ("Join these 2 channels, then tap Continue") instead of
   forcing sequential single-channel prompts — cuts up to 4 round trips
   down to 1 when multiple gates are active simultaneously.
4. File delivered with the existing auto-delete notice. Add a persistent
   one-tap "🔁 Search again" / "❤️ Save to Favorites" row on the delivered
   file itself (Favorites doesn't exist yet — see Phase 8) so the next
   action is available without navigating back to Home first.

---

## PHASE 4 — Telegram Best Practices

| Practice | Status |
|---|---|
| Inline keyboards over reply keyboards | ✅ used throughout, correctly |
| `edit_message_text`/`edit_caption` instead of send-new | ✅ mostly — `_render_results_view` correctly edits in place; the poster-upgrade delete+resend (Phase 3) is the one deliberate exception, and it's justified by a real Telegram limitation, not an oversight |
| Message auto-cleanup | ✅ files, search placeholders, welcome messages, group "no chatting" warnings, and help messages all auto-delete |
| Typing indicators (`send_chat_action`) | ❌ **not used anywhere** — a `send_chat_action("upload_document")` or `"typing"` while a search/TMDB lookup is in flight would meaningfully soften the perceived wait, essentially free to add |
| Media groups (`send_media_group`) | Not applicable today (single-file delivery model), but relevant if multi-quality-in-one-tap ever ships (Phase 8) |
| Deep links | ✅ well used — `search_`, `file_`, `req_` payloads, plus group→PM handoff links |
| Pagination | ✅ implemented cleanly, shared render path |
| Inline mode (`@bot query` from any chat) | ❌ **not implemented at all** — this is probably the single highest-leverage missing Telegram feature for this product (see Phase 8/9) |
| Telegram Mini App / WebApp | ❌ not used — a real opportunity for a richer browse/grid experience (Phase 10) |
| Spoiler tags for plot summaries | ✅ already used (`<tg-spoiler>`) — nice touch, already implemented |
| Reactions on delivered files (👍/👎 "was this the right file?") | ❌ not used — cheap signal for duplicate/mislabeled-file detection |
| Message threads / topics support (forum groups) | Not addressed — worth checking if any connected groups are forum-mode supergroups, since `reply_text`/`send_message` without a `message_thread_id` can land in the wrong topic |

---

## PHASE 5 — Performance

Ranked by actual user-facing impact, not theoretical hotness.

1. **TMDB enrichment blocks the search-render critical path, with a
   realistic worst case of ~20 seconds of added latency.**
   `plugins/filter.py` (`auto_filter`, ~lines 514-531), `plugins/start.py`
   (`_execute_search`, ~lines 112-128), and `plugins/group_connect.py`
   (`group_search`, ~lines 238-254) all do the same thing: try
   `get_movie_data(clean_title)`, and if that returns `None`, **sequentially**
   retry with `get_movie_data(query)`. `tmdb.py` sets a 10-second
   `aiohttp.ClientTimeout` per call. If TMDB is slow or briefly
   unreachable, that's up to 2×10s = 20s the user sits looking at "🔍
   Searching databases..." before anything renders — for a step (poster +
   synopsis) that's cosmetic, not essential. **Fix:** render the DB
   results immediately without waiting on TMDB at all; kick off TMDB
   enrichment as a background task and `edit_caption`/`edit_media` the
   message when (if) it resolves. This alone would make the bot feel
   dramatically faster on the median case and eliminate the worst case
   entirely.

2. **No TMDB response cache.** Every search for the same movie, by any
   number of different users, across PM and every connected group, and
   every real-time auto-post, calls TMDB fresh. TMDB's practical rate
   limits mean a bot with real traffic will start seeing failed/slow
   lookups precisely when it's most popular (i.e., when many users search
   the same trending title at once). **Fix:** a simple in-process TTL
   cache keyed by the cleaned title (12-24h TTL is plenty for movie
   metadata, which barely changes) collapses what could be thousands of
   duplicate calls per day for the same handful of trending titles into
   one.

3. **Search is not index-backed for the query shapes actually used.**
   `get_search_results()` and `admin_search_files()` both build
   case-insensitive (`$options: "i"`) regex filters. MongoDB cannot use a
   B-tree index prefix optimization for a case-insensitive regex
   regardless of anchoring — every search is a full collection scan,
   fanned across up to 5 clusters. This is masked today by dataset size
   and Atlas's in-memory working set, but it is the architectural
   bottleneck that will hurt first as the library grows past what fits in
   each free-tier cluster's RAM. **Fix (in priority order):** migrate to
   MongoDB Atlas Search (a real Lucene-backed full-text index, available
   even on shared tiers) or maintain a precomputed lowercase token array
   per document with a multikey index for exact-token lookups, falling
   back to regex only for the fuzzy tail.

4. **`get_db_size()` (`dbstats`) is called synchronously, once per
   candidate cluster, inside every save.** Cheap while clusters have
   space (short-circuits on cluster 1), increasingly expensive as earlier
   clusters fill (has to sequentially check each full cluster before
   reaching one with room). Cache sizes with a short TTL instead of
   querying live on every single write.

5. **Bulk indexing is deliberately throttled to a fixed 3-second sleep
   between every 50-message batch**, regardless of how close to a
   `FloodWait` the bot actually is. This is a safe, conservative choice,
   but it means indexing 500,000 channel messages takes a **minimum** of
   ~8.3 hours of pure sleep time even with zero errors. An adaptive
   backoff (start faster, only slow down after an actual `FloodWait`)
   would meaningfully cut onboarding time for large catalogs without
   increasing ban risk.

6. **`find_duplicate_files()`** streams every document across every
   cluster into Python-side dicts for the fuzzy-match pass — correctly
   uses async streaming cursors (not `to_list()`-everything), but still
   holds a full in-memory map of normalized names for the whole library.
   At ~1.5M+ documents this is a real, if bounded, memory spike on-demand
   (admin-triggered only, not a steady-state cost).

7. **Positive finding:** `compile_regex()`'s `lru_cache`, the precompiled
   `_STOP_WORD_RES`/`_LANG_RES`/`_QUAL_RES`/`_DUPE_*_RE` module constants,
   the `$facet`-based `get_files_by_language()` (replacing what the
   blueprint says used to be `len(LANGUAGES) × len(clusters)` sequential
   `count_documents` calls), and `maxPoolSize=50` capping cross-cluster
   connection fan-out are all genuinely correct, deliberate performance
   engineering already in place. Don't undo these while "fixing" other
   things.

---

## PHASE 6 — Security Review

| Area | Finding |
|---|---|
| **Self-updater (`plugins/updater.py`)** | 🔴 **The single biggest risk in the codebase.** `/update` pulls the entire file tree from a hardcoded public GitHub repo/branch (`c7xto/mccxmoviebot@main`) and overwrites every local file except `.env`, then `os.execv()`s into the new code — with **no commit pinning, no signature/checksum verification, no diff review step**. Any compromise of that GitHub account or repo (or a malicious merge) becomes full remote code execution on the bot host — with the Telegram bot token, all 5 MongoDB credentials, and the admin's trust — the instant any admin taps "Update Bot." Restricting the trigger to `ADMIN_ID` only protects against non-admins; it does nothing if the *supply chain* is compromised, which is the actual threat model for auto-updaters. **Fix:** at minimum, pin to a specific commit SHA an admin has to explicitly approve per update (show the commit message/diff link before applying), or drop this feature in favor of a normal `git pull` + manual restart, or CI/CD-based deploys. |
| Admin bypasses | The `admin_id` Mongo field (Phase 2, #8) doesn't grant access — not a vulnerability, but worth re-flagging here since "does changing this setting actually change who has power" is exactly a security question. |
| Callback injection / user spoofing | Callback data is Telegram-signed and scoped to the message; every handler that parses IDs out of `callback_data` does so inside `try/except` with sane fallbacks. No injection surface found — Telegram's own callback mechanism is the trust boundary here, correctly relied upon rather than re-implemented. |
| Flood / spam | Per-user 2-second cooldown exists for **text searches** only (`USER_SEARCH_COOLDOWN`, capped at 10,000 LRU entries). Callback-query taps (pagination, sort, send-file) have **no rate limit at all**. A user rapid-tapping "Next" or a file button repeatedly can't hurt the DB (reads are cheap, cached) but can spawn many concurrent `_auto_delete_file`/`_auto_delete_search` sleeping tasks. Low real risk at current scale; worth a lightweight per-callback-type cooldown if this ever needs to withstand deliberate abuse. |
| Rate limits / DOS | No application-level global rate limiter; relies entirely on Telegram's own flood control plus the search cooldown. Fine for a community bot, not fine at "1M users" scale without a proper token-bucket per user/IP-equivalent (Telegram user ID). |
| Input validation | Consistently good — every numeric parse (channel IDs, user IDs, cluster indices) is wrapped in `try/except ValueError`, every regex built from user input is `re.escape()`'d first. |
| Database safety | `file_registry`'s atomic reserve pattern is correct. `/reset_db` requires a second, typed `/confirm_reset` command — appropriately high friction for the one truly destructive action. |
| Secrets management | `.env` correctly excluded from git (per `.gitignore`) and explicitly protected from the self-updater's overwrite. TMDB key is read fresh per-call from env rather than cached in a module global that could leak into a stack trace — a small, correct choice. No secrets found logged at INFO level. |
| Config export | `export_config()` deliberately strips FSub invite-link strings down to channel IDs only (documented reasoning: a leaked backup shouldn't double as a bearer credential to join private channels) — a genuinely good, non-obvious security-conscious design decision already in place. |

---

## PHASE 7 — Feature-by-Feature Verdict

| Feature | Verdict | Why |
|---|---|---|
| Multi-cluster manual sharding | **Improve** | Works, but is the architectural ceiling (Phase 5/10). Fine for current scale, needs replacing before "millions of users." |
| `file_registry` cross-cluster uniqueness | **Keep** | Correct, necessary, well-implemented. |
| Main FSub gate | **Keep** | Standard, expected pattern for this bot category. |
| Request-FSub + Two-Stage Verification (both) | **Merge** | Two independently-built, overlapping "join more channels" gates that stack with Main FSub. Consolidate into one configurable gate ("require joining N of these M channels, sequential or all-at-once") instead of three separately-coded systems with separate admin submenus. Reduces both code surface and user-facing friction (Phase 3). |
| Sort toggle (Smart/Size/Newest) | **Keep** | Cheap, useful, already well-implemented (re-sorts cached results, no re-query). |
| Data Saver toggle | **Keep** | Genuinely thoughtful, persisted per-user, correctly handles the photo↔text message-type swap. |
| Trending searches | **Keep, then improve** | Good retention mechanic already; extend with per-user personalization (Phase 8) rather than only a single global list. |
| Series grouping / expand | **Keep** | Solves a real problem (episode-list flooding) cleanly. |
| Duplicate finder (exact + fuzzy) | **Keep, fix the dead Mongo write** | Valuable admin tool; drop the never-read `duplicate_scan_results` persistence (Phase 2 #7). |
| Cluster migration tool | **Replace** | Currently broken (Phase 2 #3 — duplicates instead of moving). Either fix properly (delete-after-copy, registry-aware) or remove the button until it's fixed — a "migration" tool that silently doesn't migrate is worse than no tool. |
| Per-group auto-delete override | **Fix or delete** | Currently dead (Phase 2 #4). Either wire it up or remove the UI so admins stop trusting a setting that does nothing. |
| Group whitelist/blacklist mode | **Fix or delete** | Currently unenforced (Phase 2 #5). Same call: wire it up (straightforward) or remove the toggle. |
| Self-updater (`/update`) | **Replace** | Convenient, but the current design (Phase 6) is a real supply-chain risk. Replace with pinned-commit approval or a standard git-based deploy flow. |
| Broadcast (preview/schedule/pin/auto-delete) | **Keep** | Well-built; scheduled broadcasts not surviving a restart is a known, honestly-labeled limitation, not a hidden bug. |
| Admin panel two-tier navigation | **Keep** | Already a deliberate, documented improvement over a flat 19-button menu — good information architecture. |
| Movie request system (manual + auto-fulfillment) | **Merge the notification logic** | Two separate code paths building near-identical "your movie is ready" messages (Phase 2 #14); consolidate into one shared helper and add the already-fulfilled check. |
| Inline mode | **Add** | Doesn't exist; see Phase 4/8/9 — likely the highest-leverage missing feature for organic growth (search-and-share into any chat without adding the bot). |
| Favorites / watchlist / search history | **Add** | Doesn't exist at all today; table-stakes retention feature for a content-discovery bot (Phase 8). |

---

## PHASE 8 — What Users At 1M-Scale Would Actually Want

Grouped, not padded — every one of these is something this specific bot
(movie/series file delivery + community) plausibly benefits from, not
generic "add AI" filler.

**Discovery & personalization**
1. Personalized "Because you watched X" row on the home panel (reuse the
   existing TMDB metadata already being fetched).
2. Per-user search history with one-tap re-search.
3. Favorites/Watchlist — save a title before it's even uploaded, get
   auto-notified (this already half-exists as the request-fulfillment
   pipeline; extend it into an explicit "follow" concept, not just typed
   requests).
4. "Continue where you left off" for multi-episode series — track last
   episode a user pulled and surface "Next episode" as a shortcut.
5. Genre/mood browsing ("Show me action movies") using TMDB genre data,
   not just literal title search.
6. "New this week" digest, opt-in, delivered as a scheduled message.
7. Trending **per language** (Malayalam trending vs Tamil trending), not
   one global list.
8. Similar-titles row ("If you liked this, try…") from TMDB's
   recommendations endpoint.
9. Actor/director search ("Fahadh Faasil movies").
10. Smart typo correction using the prefix-suggestion logic that already
    exists, extended with edit-distance matching, not just prefix match.

**Social & community**
11. Public leaderboard of top requesters / most helpful group.
12. "Rate this file" 👍/👎 after delivery — cheap quality signal, doubles as
    duplicate/mislabel detection.
13. Group leaderboards (already have `search_count` per group — surface it
    as a public "top communities" list, gamifies group growth).
14. Referral tracking via deep links ("invited by") with a lightweight
    reward (priority request handling, badge, etc.).
15. Public "recently added" channel post already exists — add a weekly
    roundup digest post instead of only one-off announcements.

**Convenience / reduced friction**
16. Inline mode (`@bot movie name` from any chat) — search without ever
    opening the bot; the single highest-leverage missing feature (Phase
    4/9).
17. One combined multi-gate join screen instead of sequential prompts
    (Phase 3/7).
18. "Send in original quality" vs "compressed" choice at delivery time,
    not just a binary Data Saver toggle.
19. Multi-select "send all seasons" for a series in one tap instead of
    one-file-at-a-time.
20. Voice-note search (Telegram supports voice messages; transcribe →
    search).
21. Forwarded-message search: forward *any* message containing a title and
    have the bot recognize and search it.
22. QR code generation for a specific file's deep link (useful for posters
    shared outside Telegram).
23. "Notify me before this file auto-deletes" — a 30-second warning ping
    instead of just deleting silently.
24. Bulk request via a single message with multiple lines ("Leo\nManjummel
    Boys\nAadujeevitham" → 3 tickets at once).
25. Localized UI language toggle (Phase 3) — persisted per-user like Data
    Saver.

**Premium / "feels expensive" without necessarily charging**
26. A visually distinct "Verified Upload" badge for files from trusted
    sources vs auto-indexed community uploads.
27. Priority request queue for active/engaged users (based on search
    frequency, not payment) — cheap to build, meaningfully improves
    perceived status.
28. Higher Data-Saver-off default poster quality (w780 vs w500 from TMDB) —
    a genuinely free visual upgrade.
29. Custom per-user notification digest time ("send me new uploads at 8pm
    my time").
30. Mini App (Telegram WebApp) grid/gallery browse mode as an alternative
    to text search — dramatically more "premium" feeling for a movie bot
    than a chat-only interface (Phase 10).

**Analytics & admin-facing (indirectly improves the user experience)**
31. Per-title demand analytics ("237 people searched for this before it
    was uploaded") to prioritize what admins index next.
32. Automatic "trending but missing" digest to admins (already have
    `missed_searches`; turn the existing "Top Missing Files" screen into a
    proactive push instead of pull-only).
33. A/B-testable welcome text (the infra for custom welcome text already
    exists; add simple variant rotation + which-variant-converts-better
    tracking).
34. Search funnel drop-off tracking (searched → viewed results → tapped a
    file → completed all gates → received file) to find where users
    actually give up.
35. Per-admin activity log for accountability in multi-admin setups.

**Retention mechanics**
36. Daily/weekly streak for opening the bot (movie-club gamification,
    low-effort, proven pattern).
37. "On this day" nostalgia posts (a title indexed exactly a year ago).
38. Scheduled personal digest ("3 movies you searched for got uploaded
    this week").
39. Push notification opt-in for a specific actor/genre/language's new
    uploads, not just fulfillment of an explicit request.
40. Seasonal/festival-themed home panel refresh (Onam, Vishu, etc. for a
    Malayalam-cinema-focused community) — cheap, culturally resonant, easy
    win for this specific audience.

**Technical/product features that indirectly delight users**
41. Faster search perceived speed via the TMDB-decoupling fix (Phase 5) —
    the single highest-ROI item on this entire list.
42. Typing indicator while searching (Phase 4) — free, removes "is this
    even working" doubt.
43. Graceful degraded mode when TMDB is down (already partially there —
    make it explicit/consistent everywhere, not just where it happens to
    fall through).
44. Search suggestions-as-you-type is not feasible in plain Telegram
    (no live keystroke API for bots) — but a Mini App search box *can* do
    this (ties back to #30).
45. Consistent multi-cluster search latency regardless of which cluster a
    file lives in (Phase 5's Atlas Search migration would fix this as a
    side effect).
46. "Report broken/wrong file" one-tap button on every delivered file,
    routed to the same log channel as missed searches.
47. Batch request fulfillment notification digest (if 5 requested titles
    land in one indexing run, one combined message instead of 5 separate
    pings).
48. Smarter duplicate-upload prevention feedback to the *uploader* (right
    now duplicate rejection is silent from the channel-poster's point of
    view too).
49. Configurable per-group search cooldown separate from the global
    per-user one — busy groups currently share the same 2-second global
    cooldown state as PM.
50. Health-check status page/command for admins showing all of Phase
    2/5/6's silent-failure modes surfaced proactively instead of
    discovered by accident (ties directly back into fixing bugs #1/#2/#3).

---

## PHASE 9 — UI Redesign

### Home panel — before/after

**Before:** Trending (if any) → Add to Group + Join Channel → Movie
Request Group (full width) → Help.

**After (reordered for "fastest path to what the user wants"):**
```
[ 🔥 Trending 1 ]  [ 🔥 Trending 2 ]      ← highest-conversion taps first
[ 🔥 Trending 3 ]  [ 🔥 Trending 4 ]
[ ⭐ My Favorites ]  [ 🕓 My History ]      ← new, once Phase 8 #2/#3 exist
[ 👥 Add To Group ]  [ 📢 Updates Channel ]
[ ⚡ Request A Movie ]
[ ℹ️ Help ]  [ 🌐 Language ]
```

### Results card — trim the debug-looking line, promote actionability

**Before:**
```
🔍 Results Found For <query>
🗣️ : <first name>
📁 Files: N - 📚 Page: X/Y
🎬 <title>
<spoiler synopsis>
⭐ rating
🗑 Auto-deletes in N mins
```

**After:** drop the `🗣️ :` line entirely (Phase 3), keep everything else,
add a single trailing row: `[ ❤️ Save ]  [ 🔁 Search Again ]` so the two
most likely next actions never require a trip back to Home.

### Admin panel — already good; two concrete refinements

The existing two-tier Content/Users&Groups/Settings/Health structure is a
correct design already (explicitly a deliberate improvement over a flat
19-button wall, per the code's own comments) — don't rebuild this.
Refinements:
- Collapse **Request-FSub** and **Two-Stage Verification** into a single
  "Verification Gates" submenu once merged (Phase 7), instead of two
  separate top-level entries under Users & Groups.
- Add a **"⚠️ Known Issues"** tile to Health & System that surfaces
  bugs #1-#6 above as live checks (e.g. "Cluster 3 is full — new uploads
  are being silently rejected" or "Whitelist mode is ON but not enforced")
  until they're actually fixed in code — turns a silent failure into a
  visible one immediately, which is strictly better even before the
  underlying bug is patched.

### Keyboards & navigation

Back-button placement is already consistent (bottom-most row, consistent
label). No change needed there. The one real gap: **no breadcrumb** for
users who arrived via a deep link straight into File Manager-equivalent
depth (not applicable to end users, but relevant if Mini App navigation
(Phase 8 #30) is ever added — that needs real breadcrumb/back-stack state,
which Telegram's own inline-keyboard model doesn't provide for free).

---

## PHASE 10 — Ideal Architecture From Scratch

If starting today, knowing everything this codebase already teaches:

### Target architecture

```
┌─────────────────────────────────────────────────────────────┐
│ Telegram Bot API (webhook, not long-polling)                 │
└───────────────────────────┬───────────────────────────────────┘
                             ▼
                  ┌────────────────────┐
                  │  Stateless API tier │  (N replicas, horizontally
                  │  (FastAPI/aiohttp)  │   scalable, no in-process state)
                  └─────────┬──────────┘
                             ▼
                  ┌────────────────────┐
                  │   Redis            │  session/pagination cache,
                  │  (shared, not      │  rate limiting, distributed
                  │   in-process)      │  locks, trending counters
                  └─────────┬──────────┘
                             ▼
        ┌───────────────────┴────────────────────┐
        ▼                                          ▼
┌──────────────────┐                    ┌────────────────────────┐
│ MongoDB Atlas     │                    │ Atlas Search /          │
│ (single properly-  │                    │ Elasticsearch index     │
│  sized cluster,    │                    │ (real full-text search, │
│  or native sharding)│                   │  not regex scans)       │
└──────────────────┘                    └────────────────────────┘
        ▲
        │
┌──────────────────┐      ┌─────────────────────┐
│ Background worker  │◄────┤ Task queue (e.g.     │
│ pool (indexing,     │     │ Celery/RQ/arq)       │
│ TMDB enrichment,    │     └─────────────────────┘
│ broadcasts)         │
└──────────────────┘

+ Structured JSON logging → Sentry/Grafana Loki
+ Metrics → Prometheus (search latency, gate pass-through rate, TMDB hit rate)
+ CI/CD (lint + tests + staged deploy) replacing self-update-from-branch
+ Admin web dashboard for anything heavier than a Telegram menu can do well
  (bulk moderation, analytics charts, config diffing) — Telegram chrome
  stays for what it's good at (quick actions), the web dashboard handles
  what it's bad at (dense tables, charts, bulk multi-select).
```

### Database design

- One real MongoDB deployment sized for the actual data volume (or a
  properly *native*-sharded cluster if it must stay on MongoDB), not five
  independent free-tier accounts manually round-robined by a 450MB
  threshold. The current design isn't wrong so much as it's a workaround
  for a budget constraint — worth being explicit that it's a cost decision
  more than an architecture decision, and should be revisited the moment
  budget allows.
- Full-text search index (Atlas Search or equivalent) instead of
  regex-scan search, addressing Phase 5's #3 at the source instead of
  scaling around it.
- Redis (or equivalent) for every piece of state currently living in a
  process-local Python dict: search sessions, trending counters, admin
  input state, cooldowns, pending broadcasts, cached duplicate scans. This
  is the one change that actually unlocks running more than one process —
  everything else in the current design already assumes (and enforces,
  via `fcntl`) exactly one process.

### Handler architecture

- Typed request/response models (Pydantic) around every DB read/write
  instead of raw dicts passed around — the current code is disciplined
  about `None` checks, but a typo in a dict key (`"log_channel"` vs
  `"logchannel"`) is a runtime-only failure today, and would be a
  type-checker-caught failure with typed config models.
- A real router/dispatch layer instead of relying on Pyrogram plugin
  load-order + `ContinuePropagation` fallthrough for stateful admin input
  (Phase 1's dependency-map note) — a single "if state starts with X, call
  handler X" dispatch table replaces three duplicate catch-all filters.
- Callback data structured and versioned (e.g. `action:v1:params`) instead
  of ad hoc `#`-delimited strings, so a future schema change to callback
  payloads doesn't silently break in-flight old messages after a
  deployment.

### Admin system

Keep the Telegram-native quick-action panel (it's good, don't throw it
away) but add a web dashboard for the handful of things Telegram chrome
is structurally bad at: reviewing/approving a self-update diff before it
applies, bulk moderation across thousands of groups, real analytics
charts (not `█`/`░` bar-strings), and searchable audit logs.

### Logging & error handling

`_log_task_crash`'s pattern (route unhandled exceptions from fire-and-
forget tasks to a visible channel) is already the right idea — formalize
it into structured logging (JSON, with request/user/correlation IDs) shipped
to a real aggregator, so "search all errors for user 12345 today" is a
query, not a `Ctrl+F` through log-channel messages.

---

## Current vs Ideal — what actually changes and why

| Dimension | Current | Ideal | Why change |
|---|---|---|---|
| Process model | Single process, `fcntl`-locked, long-polling | Stateless horizontally-scaled workers behind a webhook | Long-polling + a filesystem lock structurally cannot scale past one instance; webhooks + statelessness can. |
| Search | Regex scan, fanned across 5 clusters | Real full-text index (Atlas Search) | Regex with `$options:"i"` can't use a B-tree index at all — this is the architecture's real ceiling. |
| Session/cache state | In-process Python dicts | Redis | Unlocks multi-process/multi-region deployment; today's design is single-point-of-failure by construction. |
| Storage sharding | Manual round-robin across 5 free-tier clusters | One properly-provisioned deployment (or native sharding) | Current design is a budget workaround, not an architecture choice — say so plainly. |
| Deploys | `/update` pulls an unpinned branch and `os.execv()`s | CI/CD with review/staging, or at minimum pinned-commit approval | Phase 6's biggest finding — supply-chain RCE risk. |
| Admin UI | Telegram menus only | Telegram menus (kept) + web dashboard for dense/bulk operations | Telegram chrome is genuinely good for quick actions, genuinely bad for tables/charts/bulk review. |
| TMDB calls | Live, uncached, blocking search render | Cached, decoupled from render path | Directly fixes the worst latency bug found in this audit. |
| Observability | Log-channel messages + `_log_task_crash` | Structured logs + metrics + the same crash-alert pattern, formalized | Keep the good idea, give it a real backend. |

---

## PHASE 11 — Prioritized Refactor Roadmap

### Critical (fix before anything else — silent data loss / real security exposure)
- Fix bug #1: `save_files_bulk` silent full-database failure. *Effort: S (half day).*
- Fix bug #2: `index_new_files` missing failure-path alert. *Effort: XS (1-2 hours).*
- Fix bug #3: `migrate_cluster` duplicating instead of moving. *Effort: M (1 day incl. testing against a real cluster pair).*
- Pin or gate the self-updater (Phase 6). *Effort: M (1 day) for commit-pinning; L (2-3 days) to replace with a proper CI/CD flow.*

### High (real user-facing or admin-trust impact)
- Decouple TMDB from the search-render critical path (Phase 5 #1). *Effort: M (1 day) — biggest perceived-speed win available.*
- Add a TMDB response cache (Phase 5 #2). *Effort: S (half day).*
- Wire up or remove: per-group auto-delete override, whitelist-mode enforcement, `admin_id` field (Phase 2 #4/#5/#8). *Effort: S each.*
- Consolidate Request-FSub + Two-Stage Verification into one gate system (Phase 3/7). *Effort: L (2-3 days) — touches admin UI, DB schema, and both delivery paths.*

### Medium (maintainability, won't bite anyone today but will bite the next contributor)
- Deduplicate `_no_preview()`/`_html()` into shared modules (Phase 2 #9/#10). *Effort: S (half day).*
- Rename `index.py`/`indexer.py` to something unambiguous (Phase 2 #11). *Effort: XS.*
- Remove stale command-exclusion entries (`about`, `purge_cams`, `connect`) (Phase 2 #12). *Effort: XS.*
- Drop the dead `duplicate_scan_results` Mongo write (Phase 2 #7). *Effort: XS.*
- Consolidate the two "movie ready" notification code paths (Phase 2 #14). *Effort: S.*

### Low (cosmetic / nice-to-have cleanup)
- Clean up ticket-number-style comments (Phase 2 #15).
- Cache cluster `dbstats` sizes with a short TTL (Phase 2 #16 / Phase 5 #4). *Effort: S.*
- Drop the `🗣️ : {first_name}` line from the results caption (Phase 3/9). *Effort: XS.*

### Quick wins (disproportionate payoff for the effort)
- Typing indicator during search (Phase 4). *Effort: XS.*
- Reorder home panel to lead with Trending/Favorites over promo buttons (Phase 9). *Effort: XS.*
- "Report broken file" one-tap button (Phase 8 #46). *Effort: S.*
- Health & System "Known Issues" live-check tile (Phase 9). *Effort: S — and it makes every Critical/High bug visible to the admin immediately, even before each is individually fixed.*

### Long-term (the Phase 10 rebuild)
- Full-text search migration (Atlas Search). *Effort: XL (1-2 weeks incl. reindexing ~1.5M+ documents and dual-running to validate result parity).*
- Redis-backed shared state, enabling multi-process/webhook deployment. *Effort: XL (2-3 weeks) — genuinely the project that unlocks "1M users," everything else is optimization within a single-process ceiling.*
- Mini App browse experience. *Effort: L-XL depending on scope (1-3 weeks).*
- Structured logging/metrics stack. *Effort: M-L (1 week).*

---

## Closing note

The strongest thing this codebase has going for it is that it's already
self-aware — the blueprint document, the crash-callback convention, the
`file_registry` fix for a real prior bug, all show a team that iterates on
its own mistakes. The three silent-failure bugs found here (#1-#3) are the
kind that specifically hide *because* the rest of the error handling is so
consistently good elsewhere — they're the exceptions to an otherwise solid
pattern, not evidence the pattern is missing. Fix those three first; they
are actively costing files and cluster space today, silently, and nothing
else in this audit comes close to that level of real, ongoing harm.
