# MCCxMovieBot Security, Architecture, and Performance Audit

**Audit date:** 2026-08-27  
**Repository commit:** `fd83b29bdfd7d570915bdae01381d30f57357b57`  
**Scope:** all 41 tracked files, all 26 Python files, container/CI configuration, dependency manifests, Git history relevant to secrets, and local runtime artifacts that can enter a build context.  
**Method:** manual control-flow and data-flow review, handler and sink enumeration, AST-based async/task scan, secret-pattern scan, Git-history checks, Ruff, `pip-audit`, and repeated independent review passes.

## Executive summary

The bot has one confirmed **Critical** issue: the current Docker build context includes a populated Pyrogram session database and its journal. `Dockerfile` copies the entire context, while `.dockerignore` does not exclude session files. A built or published image can therefore contain reusable Telegram authorization material even though Git correctly ignores it.

The most important application risks are authorization policy being enforced at search time rather than at the final delivery boundary, several verification checks failing open on dependency errors, an easily bypassed Request-FSub interval, a bulk-index checkpoint that advances after failed writes, a duplicate-deletion UI that deletes the copy it says it will keep, and consistency hazards in the sharded file registry. Public group searches also have no meaningful workload controls and fan out expensive regex scans to every configured database cluster.

No tracked credential or private-key pattern was found, and no relevant secret file appeared in Git history. The pinned runtime dependencies returned no known vulnerability from `pip-audit` on the audit date. The normal movie path stores Telegram `file_id` metadata and uses `send_cached_media`; it does **not** download user-named movie files to local disk, so no direct path-traversal sink was found in media ingestion or delivery. There are no inline-query handlers in this version.

### Finding count

| Severity | Count |
|---|---:|
| Critical | 1 |
| High | 9 |
| Medium | 12 |
| Low | 6 |
| **Total** | **28** |

## Architecture and trust boundaries

### Runtime components

- `bot.py:61-144` loads environment configuration, validates startup settings, creates the Pyrogram client, and enables plugin auto-loading. `bot.py:145-224` initializes MongoDB, migrations, indexes, caches, and four managed background workers.
- `database/db.py:371-443` creates as many as five independent `AsyncMongoClient` pools. Movie metadata is sharded across each cluster's `movies` collection. Operational collections, including users, config, registry, search/index state, and deletion jobs, live on the selected operations cluster.
- `plugins/realtime_indexer.py:195-250` indexes new document/video/audio channel posts. `plugins/bulk_indexer.py:22-241` performs historical indexing in message batches.
- `plugins/filter.py:685-1118` serves private-message search, result navigation, filtering, and file callbacks. `plugins/group_connect.py:123-365` serves group search and deep-links users back to private chat. `plugins/start.py:212-326` dispatches deep links.
- `plugins/req_fsub.py:165-330` combines Main FSub, Request-FSub, and two-stage membership gates, then performs a final cached-media send.
- `plugins/broadcast.py:21-128` copies an admin-supplied Telegram message to users and/or groups. `plugins/realtime_indexer.py:37-190` generates release announcements and request fulfillment messages.
- `plugins/updater.py:45-482` reviews a GitHub commit, downloads and compiles a staged tree, optionally installs dependencies, applies it to the live tree, and restarts the process.

### Media and data flow

```text
Authorized Telegram database channel
  -> Pyrogram media object
  -> normalize filename and capture file_id/size/MIME
  -> reserve file_id in operations DB registry
  -> insert metadata into one movie shard
  -> optional announcement/request-fulfillment tasks

User/group query
  -> query normalization
  -> regex fan-out to every movie shard
  -> in-process ranking/deduplication
  -> bounded in-memory search session containing result documents
  -> ObjectId in callback/deep link
  -> retrieve shard record
  -> subscription/verification gates
  -> Telegram send_cached_media(file_id)
  -> durable MongoDB auto-deletion job
```

The bot does not proxy movie bytes through Python memory or save movie files locally. Telegram/Kurigram performs cached-media delivery using `file_id`. The main large-memory exceptions are the admin JSON restore, full title/duplicate scans, updater downloads, and list-materializing administration/broadcast methods.

### Trust boundaries

- Public Telegram messages, commands, callbacks, and deep-link payloads are untrusted. Callback data is replayable and can be manually constructed; it must not be treated as authorization.
- Telegram channel media metadata is trusted only after its chat ID is matched against configured database channels.
- Administrators are privileged, but stale callbacks, accidental input, and admin-account compromise must still be contained because update, restore, purge, migration, and broadcast operations have high impact.
- MongoDB, Telegram, GitHub, and TMDB are remote dependencies. Errors and rate limits must not silently turn authorization checks into successful checks.
- `.env`, Pyrogram session files, the runtime catalog, and updater manifests are host runtime state and must never be part of an immutable image or distributable artifact.

## Critical findings

### C-01 — Live Pyrogram session credentials enter the Docker image

**Evidence:** `Dockerfile:13` executes `COPY --chown=mccx:mccx . .`. `.dockerignore:1-11` excludes `.env` but has no rule for `*.session`, `*.session-journal`, `runtime/`, `.deployed_sha`, or `.deployed_files.json`. `bot.py:126-136` uses the predictable relative session name `MCCxBot`. At audit time, `MCCxBot.session` and `MCCxBot.session-journal` existed in the build context, and the SQLite session database contained an initialized session row and authentication-key field. Values were not extracted into this report. `.gitignore:1-12` correctly excludes these files from Git, but Git ignore rules do not control Docker.

**Impact:** Anyone who can pull or export a built image, access its layers/cache, or receive an image archive may recover reusable Telegram session authorization material and impersonate the bot. The runtime catalog and deployment manifests are also unintentionally disclosed and bloat the image.

**Recommended change:** First add `*.session`, `*.session-journal`, `runtime/`, `.deployed_sha`, and `.deployed_files.json` to `.dockerignore`, and add an automated build-context test. Store the session on a runtime-only volume or use an explicitly supplied protected session location/string. Rebuild from a clean context and remove old local/registry image layers. If any existing image left the trusted host, revoke the Telegram session and rotate the bot token before redeployment.

## High findings

### H-01 — Ban and maintenance controls are bypassable through delivery and deep-link paths

**Evidence:** `plugins/filter.py:695-703` checks ban and maintenance state before normal private search. The actual delivery handlers at `plugins/filter.py:940-982`, `plugins/filter.py:1002-1118`, and `plugins/req_fsub.py:302-330` do not repeat that policy. `plugins/start.py:284-326` reads data and dispatches `file_`, `req_`, and arbitrary search payloads without a ban or maintenance check; `plugins/start.py:212-249` sends deep-linked files. A banned user can reuse an old callback, open a group-generated `file_<ObjectId>` link, or use `/start` payloads. Maintenance mode similarly does not prevent final delivery.

**Impact:** Bans and maintenance mode do not reliably stop access, requests, database load, or media delivery.

**Recommended change:** Create one `authorize_user_action(user_id, action, config)` policy function and call it at the final delivery/request/search boundary, not only in the UI entry handler. Every route that reaches `send_cached_media` should call the same delivery function, which must enforce ban, maintenance, throttling, and verification immediately before sending. Add matrix tests for private buttons, old FSub buttons, verification callbacks, group deep links, arbitrary `/start` payloads, and `/request`.

### H-02 — Verification gates fail open when Telegram or MongoDB is unavailable

**Evidence:** Main FSub catches any non-`UserNotParticipant` exception and continues at `utils.py:109-136`. Request/two-stage membership returns `True` on any generic error at `plugins/req_fsub.py:90-115`; `plugins/req_fsub.py:204-214` also maps gathered exceptions to “joined.” Legacy FSub does the same at `plugins/filter.py:985-1031`. Due-state database failures return “not due” at `database/db.py:1933-1967`. These generic paths include authorization failures, invalid channel identifiers, network errors, and `FloodWait` above Pyrogram's automatic threshold.

**Impact:** A transient dependency outage or bad configuration can silently grant access to content that operators intended to gate.

**Recommended change:** Return a tri-state result (`PASS`, `DENY`, `INDETERMINATE`) from every gate. Deliver only on `PASS`; on `INDETERMINATE`, provide a temporary retry message without changing verification timestamps. Handle `FloodWait` explicitly with bounded retry, emit structured metrics/logs, and distinguish permanent configuration errors from transient Telegram failures.

### H-03 — Request-FSub can be bypassed by ignoring one prompt, and private links cannot be verified

**Evidence:** `plugins/req_fsub.py:219-227` calls `mark_req_fsub_shown` when an unjoined channel prompt is displayed, before membership is confirmed. `database/db.py:1958-1975` then suppresses the gate for the configured interval. `plugins/admin.py:952-991` stores a private `https://t.me/+...` invite URL directly as the channel identifier. `plugins/req_fsub.py:90-114` passes that URL to `get_chat_member`; the resulting error is converted to “joined” by the fail-open path.

**Impact:** A user can dismiss the first prompt and fetch another file during the interval. Private invite-link entries are effectively unverifiable and normally pass automatically.

**Recommended change:** Store a verified numeric chat ID separately from its invite link. Resolve and validate public identifiers while the bot is known to have access; require a numeric ID plus link for private channels. Update the interval only after the verification callback confirms membership in every required channel. Add tests for ignored prompts, a second file during the interval, invalid/private link entries, and dependency errors.

### H-04 — Bulk indexing permanently skips a failed database batch

**Evidence:** `plugins/bulk_indexer.py:152-183` logs a generic `save_files_bulk` exception as non-fatal. `plugins/bulk_indexer.py:185` advances the durable checkpoint to `end_id` regardless of that failure.

**Impact:** A transient MongoDB failure can permanently skip every media record in a batch. Resuming starts after the failed range, producing silent catalog data loss.

**Recommended change:** Advance the checkpoint only after the batch has an acknowledged disposition for every item (saved or known duplicate). Retry transient failures with exponential backoff and jitter; persist failed ranges or a dead-letter record after the retry budget. Treat checkpoint persistence failure as a stop condition rather than continuing with an uncertain resume point.

### H-05 — Per-group duplicate deletion removes every copy, including the “kept” copy

**Evidence:** `database/db.py:1249-1317` returns every ObjectId in each duplicate group. `plugins/file_manager.py:250-273` incorrectly states that the oldest ID was already removed, places all IDs into the button, and `plugins/file_manager.py:173-205` deletes each one. The separate delete-all path correctly chooses the oldest at `database/db.py:1319-1335`, proving the per-group path has different behavior. Joining even three 24-character ObjectIds also exceeds Telegram's 64-byte callback-data limit.

**Impact:** An administrator clicking the advertised “delete extras, keep oldest” button can delete all catalog copies of that title. Larger groups may produce invalid buttons or unhandled UI failures.

**Recommended change:** Do not place document-ID lists in callback data. Store a short server-side scan/group token, re-fetch the group on confirmation, calculate the oldest record at deletion time, and delete only the remainder. Make the operation idempotent and add a second confirmation showing the exact keep/delete counts.

### H-06 — Registry metadata failure can orphan a successful physical insert

**Evidence:** In `database/db.py:892-946`, the physical insert (`:916`) and registry-location update (`:917-921`) share one `try`. If the insert succeeds but the registry update fails, execution continues to other shards as if the insert failed. After exhausting the loop, `:941-945` deletes the registry reservation even though a physical movie row may exist.

**Impact:** The same `file_id` can be inserted again, records can become invisible to the registry, and later cleanup/migration decisions can corrupt cross-shard uniqueness.

**Recommended change:** Separate the physical insert outcome from the metadata-enrichment outcome. Once an insert is acknowledged, return success and queue a registry-location repair rather than retrying the insert. Add an idempotent reconciliation worker that compares registry claims with physical rows and repairs missing/incorrect locations. Fault-injection tests should fail each write between reservation, insert, metadata update, and rollback.

### H-07 — Required uniqueness indexes are optional at startup

**Evidence:** `database/db.py:518-546` logs and continues when shard indexes cannot be created. `database/db.py:576-581` does the same for the central `file_registry.file_id` unique index. The reservation design at `database/db.py:900-908` and `database/db.py:948-1000` assumes duplicate inserts are atomically rejected.

**Impact:** Starting without the registry unique index removes the concurrency guarantee on which cross-cluster deduplication depends. Multiple workers can reserve the same file and create inconsistent copies.

**Recommended change:** Classify indexes as required or optional. Verify both key and `unique: true` after creation and fail startup/readiness if the registry invariant is absent. Provide an explicit repair/migration command for pre-existing duplicates rather than silently running in a degraded state.

### H-08 — Public searches and file sends lack workload controls

**Evidence:** `plugins/group_connect.py:123-180` processes nearly every group text message of three or more characters and has no per-user, per-group, or global rate limiter. Private search has only a two-second, process-local cooldown at `plugins/filter.py:709-728`. File callbacks at `plugins/filter.py:940-982` and deep links at `plugins/start.py:212-249` have no cooldown or in-flight deduplication. Each miss can perform multiple case-insensitive regex fan-outs to as many as five clusters at `database/db.py:1438-1558`, with an up-to-nine-second budget per shard. `database/db.py:392-407` permits 50 connections per cluster.

**Impact:** Ordinary group chatter or intentional callback/search floods can saturate MongoDB pools, consume event-loop CPU, and exhaust Telegram/API quotas. The bot can become unavailable to all users without a sophisticated attacker.

**Recommended change:** Add distributed or database-backed token buckets per user and group, a global search semaphore/queue, a maximum normalized query length/token count, and an in-flight/idempotency guard for file delivery. Replace scan-oriented regex search with Atlas Search or indexed normalized token/prefix fields. Instrument queue depth, per-route latency, timeouts, and rejection counts.

### H-09 — Self-update is not atomic across code, dependencies, callbacks, or concurrent runs

**Evidence:** `plugins/updater.py:87-103` ignores GitHub's recursive-tree `truncated` flag. `plugins/updater.py:224-246` installs dependencies before the live-source backup/apply transaction, but rollback at `:263-301` covers only files. `plugins/updater.py:253-295` trusts paths from the previous local manifest and performs some filesystem operations synchronously in the event loop. `plugins/updater.py:467-482` accepts a reusable confirmation callback without checking that it matches a current review state and starts an untracked task without a global update lock. `_download` buffers full files at `plugins/updater.py:106-111`, with up to eight concurrent downloads at `:190-203` and no size limit.

**Impact:** Two admin clicks can race live-tree mutation; a truncated tree can trigger wrong stale-file decisions; dependency changes survive a source rollback; stale buttons can execute previously reviewed SHAs; and unexpectedly large repository blobs can exhaust memory. An interrupted update may leave a deployment that the success/rollback text does not accurately describe.

**Recommended change:** Prefer immutable image deployment over in-process self-update. If retained, serialize updates with a lock, bind a one-time expiring review nonce to the resolved full commit SHA, reject truncated trees, cap total/per-file download size, validate all current and prior-manifest paths after resolving symlinks under `PROJECT_ROOT`, and move all filesystem work off the event loop. Build/test dependencies in a new virtual environment or image and switch releases atomically; never mutate the active environment before the release is ready.

## Medium findings

### M-01 — Full-catalog and duplicate operations block the event loop and can consume large memory

**Evidence:** `database/db.py:804-875` scans all movie rows, builds a set of unique titles, synchronously sorts it at `:856`, and writes a gzip catalog. `database/db.py:270-296` runs RapidFuzz extraction across the entire catalog synchronously; it is called in the user request path at `database/db.py:1536-1557`. `database/db.py:1249-1317` materializes normalized data for every named file across every shard and sorts the results in Python.

**Impact:** A large library or typo-heavy traffic can pause all Pyrogram handlers, while duplicate scans can temporarily approach the memory footprint of the catalog itself plus Python object overhead.

**Recommended change:** Move catalog sorting, RapidFuzz extraction, gzip I/O, and duplicate normalization to a worker thread/process with explicit concurrency limits. Prefer incremental/index-backed title and duplicate keys. Stream or aggregate duplicate candidates in MongoDB and paginate results instead of retaining the entire corpus in process.

### M-02 — FloodWait handling is inconsistent on core paths

**Evidence:** `bot.py:129-136` sets `sleep_threshold=60`, so larger waits still raise. Broadcast, bulk indexing, admin link refresh, and deletion workers handle some `FloodWait` cases (`plugins/broadcast.py:21-78`, `plugins/bulk_indexer.py:95-111`, `plugins/admin.py:1210-1234`, `plugins/health_monitor.py:263-288`). Core file delivery, search indicator/UI edits, group search, request notifications, and update-channel posting do not. `plugins/realtime_indexer.py:173-190` catches all posting errors and drops the announcement; `plugins/filter.py:11` and `plugins/group_connect.py:8` import `FloodWait` without using it.

**Impact:** Rate limiting above 60 seconds can become a failed delivery, lost announcement, or noisy task error. Independent retry loops can also synchronize and create retry bursts.

**Recommended change:** Wrap Telegram operations in a shared bounded retry policy with jitter, cancellation awareness, maximum elapsed time, and operation-specific idempotency. Queue optional notifications durably; do not retry interactive responses indefinitely. Export FloodWait duration/count metrics.

### M-03 — Long-running and fire-and-forget tasks are not lifecycle-managed

**Evidence:** `bot.py:194-221` tracks and cancels four startup workers. Index, reset-index, migration, duplicate scan, scheduled broadcast, realtime queue, delayed enqueue, fulfillment, and update tasks are created elsewhere and are not added to that set: `plugins/bulk_indexer.py:347-370`, `plugins/file_manager.py:142-152`, `plugins/file_manager.py:399-410`, `plugins/broadcast.py:100-126`, `plugins/realtime_indexer.py:49-55`, `plugins/realtime_indexer.py:239-250`, and `plugins/updater.py:467-482`. Several logging tasks also have no done callback.

**Impact:** Shutdown can leave operations mid-batch or mid-mutation, scheduled work is silently lost, and exceptions/cancellations are inconsistently observed. A restart during migration/update has a wider inconsistency window.

**Recommended change:** Introduce an application task supervisor/task group. Register every long-lived/background task with ownership, cancellation and completion handling; block conflicting per-channel/per-operation jobs; and let shutdown stop intake, drain bounded queues, then cancel/await tasks within a deadline.

### M-04 — Realtime announcement work has an unbounded queue and task fan-out

**Evidence:** `plugins/realtime_indexer.py:29-55` creates `asyncio.Queue()` without `maxsize` and drains one item every three seconds. Every successful indexed file creates a sleeping enqueue task plus a fulfillment task at `plugins/realtime_indexer.py:239-250`. `database/db.py:2015-2028` can materialize every pending request matching broad five-character prefixes.

**Impact:** A burst can grow queue memory and task count much faster than it drains, then create a long stale-announcement backlog. Broad matches can amplify one upload into many API sends.

**Recommended change:** Use a bounded queue and explicit coalescing/drop/backpressure policy; run one supervised producer/consumer pipeline; limit and page request matches; cap fulfillment concurrency; and persist jobs that must survive restart.

### M-05 — File retrieval ignores registry location and can accumulate 30-second shard waits

**Evidence:** `database/db.py:1612-1621` queries each shard sequentially by ObjectId. MongoDB client timeouts are 30 seconds at `database/db.py:392-407`, while the registry already stores `cluster` and `movie_id` at `database/db.py:917-921`.

**Impact:** A missing file or unavailable early shard can delay an interactive delivery for tens of seconds per cluster and tie up many handler coroutines.

**Recommended change:** Look up the registry location first and query only the indicated shard. Use a small bounded timeout and parallel fallback only for legacy/unreconciled records. Add an index or direct registry mapping for ObjectId-based delivery.

### M-06 — Admin configuration restore buffers and parses an unbounded document

**Evidence:** `plugins/admin.py:1359-1394` validates only the filename suffix, downloads the whole Telegram document with `in_memory=True`, copies it with `getvalue()`, decodes it, and synchronously parses JSON. No `file_size`, root-type, key-count, nesting-depth, or schema limit is enforced.

**Impact:** An accidental large upload—or a compromised administrator—can exhaust bot memory or block the event loop. Unexpected JSON shapes are passed into the restore layer.

**Recommended change:** Reject documents above a small explicit limit before download, parse off the event loop, require a mapping root, enforce an allowlisted typed schema and bounded nesting/key counts, and show a diff/confirmation before applying.

### M-07 — Remote MongoDB transport encryption is optional

**Evidence:** `database/db.py:385-408` enables TLS only for `mongodb+srv://` or when `tls=true`/`ssl=true` already appears in the URI. A remote `mongodb://` URI without those flags is accepted.

**Impact:** A deployment mistake can send database credentials and bot data over an unencrypted connection.

**Recommended change:** Require TLS for every non-loopback MongoDB endpoint and fail startup on an insecure URI unless an explicit development-only override is set. Validate certificates with the configured CA and document the supported URI format.

### M-08 — Two-stage verification success is never cached

**Evidence:** `plugins/req_fsub.py:247-254` adds `mark_two_stage_verified` only while at least one stage is missing. On the verification callback, the user has joined, so `_collect_outstanding_gates` returns no missing stages and no mark function; `plugins/req_fsub.py:322-323` therefore has nothing to call. The database method at `database/db.py:1946-1952` has no other call site.

**Impact:** Users who passed two-stage verification are checked against Telegram again for every file despite the advertised 30-minute interval, adding API load and increasing exposure to FloodWait/fail-open behavior.

**Recommended change:** Return explicit gate state independent of the missing-link list and mark the two-stage timestamp whenever a due gate is conclusively passed. Test first pass, callback pass, cached pass, expiry, and database failure.

### M-09 — Durable auto-delete jobs are discarded after three generic failures

**Evidence:** `plugins/health_monitor.py:263-288` deletes a job record after `attempts >= 2` for any non-FloodWait exception, with no dead-letter collection or operator alert.

**Impact:** A transient Telegram outage or temporary permission issue can leave promised auto-deleted media/messages in user chats indefinitely, with only a warning in process logs.

**Recommended change:** Classify permanent versus transient Telegram errors, use exponential backoff for transient failures, retain exhausted jobs in a dead-letter collection, and alert operators with chat/message identifiers and a retry action.

### M-10 — Group broadcasts materialize recipients and scheduled broadcasts are volatile

**Evidence:** `database/db.py:686-690` returns all group documents as a list; `plugins/broadcast.py:61-78` uses it for group broadcasting. `plugins/broadcast.py:100-126` holds the message and schedule in process memory, sleeps for the full delay, and explicitly warns that restart cancels it. Scheduled tasks are not supervised.

**Impact:** Large group lists create avoidable memory pressure. Restarts lose scheduled broadcasts and retained Telegram message objects, with no durable audit/idempotency state.

**Recommended change:** Stream groups in batches as users already are, persist scheduled jobs and status, and use idempotent recipient checkpoints so restart resumes rather than repeats or loses a broadcast.

### M-11 — Private invite links are written to logs and incompletely removed from config exports

**Evidence:** `database/db.py:583-606` logs migrated environment values verbatim, including `update_channel` and `main_group` URLs. `database/db.py:1745-1758` removes FSub invite links from exported config but retains other configured links. Private Telegram invite links act as bearer credentials.

**Impact:** Anyone with log or backup access may gain unintended channel/group access, and exported files can be redistributed beyond their intended audience.

**Recommended change:** Log key names and redacted value types only. Treat every `t.me/+` link as a secret, redact it from exports by default, and support deliberate encrypted secret export separately.

### M-12 — Security-critical behavior lacks executable regression coverage in the audited environment

**Evidence:** Existing tests (`tests/test_database_invariants.py`, `tests/test_startup_compat.py`, `tests/test_ui_formatting.py`) cover selected database/search/startup/UI behaviors but not delivery authorization, gate error states, Docker context, duplicate button deletion, bulk-index checkpoint failure, updater transactions, config size limits, or FloodWait behavior. The local `.venv` could not collect tests because `rapidfuzz` was missing; the system interpreter lacked pytest. CI installs `requirements-dev.txt` at `.github/workflows/quality.yml:16-19`, but the audit could not confirm a test run from the current environment.

**Impact:** The highest-risk invariants can regress without CI detection, and the checked-out environment is not reproducibly ready to run its own test suite.

**Recommended change:** Add focused unit/fault-injection tests for every Critical/High item, a Docker build-context assertion, and async tests that simulate Telegram/Mongo/FloodWait failures. Build a clean locked environment in CI and make local setup reproducible with one documented command.

## Low findings

### L-01 — User-controlled Markdown and raw exceptions can spoof messages or leak details

**Evidence:** `plugins/request.py:75-103` interpolates movie titles into Markdown and returns raw exception text to the requester. Similar logging interpolations occur at `plugins/filter.py:761-767` and `plugins/group_connect.py:194-199`. HTML paths generally use `utils.py:22-26`, but Markdown paths have no common escaping helper.

**Impact:** Crafted titles/queries can alter formatting, create misleading links/mentions in admin logs, or expose backend error details. This is message injection, not Python/code injection.

**Recommended change:** Prefer HTML with `_html()` everywhere, or add one tested MarkdownV2 escape helper. Send users stable error codes and keep full exceptions in structured server logs with secrets redacted.

### L-02 — The process lock uses a predictable, symlink-following temporary path

**Evidence:** `bot.py:94-110` opens `tempfile.gettempdir()/mccxbot.lock` with `open(..., "a+")` before locking it.

**Impact:** On a shared host, another local user can pre-create a symlink to a bot-writable file, causing a one-byte append or denial of service. Container isolation and the non-root user reduce exposure.

**Recommended change:** Use a runtime directory owned by the bot with restrictive permissions and a no-follow/exclusive-create-capable lock implementation. Validate ownership and file type before use.

### L-03 — TMDB credentials are placed in the URL and connections are not pooled

**Evidence:** `tmdb.py:63-75` builds `api_key=<secret>` into the query URL and opens a new `aiohttp.ClientSession` for every uncached lookup.

**Impact:** Query URLs are more likely than headers to appear in proxy/trace logs, and repeated TLS/session setup wastes sockets and latency.

**Recommended change:** Use TMDB's bearer authorization header where supported and keep one application-owned `ClientSession` with explicit connector limits/timeouts, closed during bot shutdown.

### L-04 — Container and CI supply-chain hardening is incomplete

**Evidence:** `Dockerfile:1` uses the mutable `python:3.13-slim` tag, `Dockerfile:11` upgrades/install packages without hashes, and `.github/workflows/quality.yml:11-12` references actions by mutable major tags. `docker-compose.yml:1-7` has no healthcheck, read-only root filesystem, capability drop, resource limits, or explicit session volume.

**Impact:** Builds are less reproducible, upstream tag compromise has a wider blast radius, and a compromised bot process has more writable/container resources than necessary.

**Recommended change:** Pin image and actions by digest/commit, generate hash-locked dependencies, add least-privilege container settings and resource limits, mount only required writable runtime paths, and add a readiness healthcheck.

### L-05 — Cache invalidation and stale-index monitoring create avoidable churn/noise

**Evidence:** `database/db.py:798-802` clears the whole query cache whenever the file count is invalidated, so active realtime ingestion can defeat search caching. `plugins/health_monitor.py:255-260` reaps only `_search_cache`, although the query cache is separately bounded. `database/db.py:2047-2053` updates checkpoint progress without refreshing the index-task `updated` timestamp set at `database/db.py:2073-2079`; `plugins/health_monitor.py:224-238` can therefore flag a legitimate long index as stale.

**Impact:** Ingestion bursts cause extra database scans, and healthy long-running jobs can generate false alerts.

**Recommended change:** Version or selectively invalidate query-cache entries, expose cache hit/eviction metrics, and update a dedicated heartbeat on every successful index batch.

### L-06 — The updater's lexical path check does not defend against local symlink/manifest tampering

**Evidence:** `plugins/updater.py:60-64` rejects absolute paths and `..` for repository tree entries, but apply/rollback paths at `plugins/updater.py:253-295` are not resolved and checked against `PROJECT_ROOT`. Previous-manifest paths are used without `_safe_relative`, and existing symlink parents/destinations are not rejected.

**Impact:** An attacker who already has local write access to updater state or the live tree could redirect an update write/delete outside the repository. No tracked symlink was present at audit time, so this is defense-in-depth rather than a public remote path-traversal finding.

**Recommended change:** Validate every path from both GitHub and local manifests, reject symlink components, resolve the intended parent, and assert it remains under `PROJECT_ROOT` immediately before every write, copy, or delete.

## Input-validation and path-traversal conclusions

- No inline-query handler exists (`on_inline_query` has no occurrences), so inline-query validation is not applicable to the current code. If inline mode is added, it needs its own length, rate, and result-size controls.
- Private `/request` limits the command title to 40 characters at `plugins/request.py:28-34`, and `utils.py:29-35` safely truncates callback payloads on UTF-8 boundaries. Search handlers do not impose a comparable maximum normalized length beyond Telegram's message/payload limits; this is included in H-08.
- User query fragments used in MongoDB regexes are escaped in `database/db.py:1164`, `database/db.py:1487-1508`, and `database/db.py:1588-1594`. The concern is scan cost, not regex injection.
- Media ingestion at `plugins/realtime_indexer.py:195-250` and `plugins/bulk_indexer.py:22-241` stores filename metadata and Telegram IDs; it does not pass filenames to local filesystem APIs.
- The config restore is in-memory (`plugins/admin.py:1376-1378`) and therefore is an OOM/schema risk, not a filename traversal sink.
- The title catalog writes only fixed internal paths at `database/db.py:250-266` and `database/db.py:804-875`.
- The updater has a lexical traversal check and fixed repository root, but needs the symlink/manifest hardening described in L-06.
- No `eval`, `exec`, unsafe pickle loading, `shell=True`, or command construction from public user input was found. The updater uses `asyncio.create_subprocess_exec` with fixed interpreter/pip arguments at `plugins/updater.py:114-135`.

## Session, memory, and rate-limit assessment

- **Session:** Pyrogram persists an authorization session in the current working directory under `MCCxBot`; the critical problem is packaging that state. The session is Git-ignored, and no session file was found in relevant Git history. Use a runtime-only, permission-restricted mount or managed secret rather than the source/build directory.
- **Movie transfer memory:** normal movie indexing/delivery does not load media bytes in bot memory. Telegram file references are persisted and sent with `send_cached_media`. The configured `max_concurrent_transmissions=3` at `bot.py:129-136` provides some Pyrogram transfer concurrency control.
- **Non-movie memory:** config restore buffers the full attachment; updater downloads buffer whole files with concurrency eight; full title and duplicate scans materialize large Python collections; realtime announcements use an unbounded queue. These are addressed in H-09, M-01, M-04, and M-06.
- **Database connections:** up to five clusters with `maxPoolSize=50` allow a theoretical 250 pooled connections. Public searches fan out across all clusters. A global workload limiter should be sized below both Atlas and Telegram constraints.
- **Rate limits:** Pyrogram automatically sleeps only up to the configured 60-second threshold. Explicit handling is fragmented and not idempotent across all operations; see M-02.

## Positive controls observed

- `.env`, `*.session`, and `*.session-journal` are excluded by `.gitignore`; tracked-file and relevant-history scans found no matching credentials.
- All administrator mutation handlers reviewed use `filters.user(ADMIN_ID)`, including reset, restore, update, broadcast, file management, group management, and index control.
- The client uses Pyrogram/Kurigram async APIs and the database layer uses `AsyncMongoClient`; no ordinary synchronous HTTP client was found in handlers.
- Search result pagination/filter callbacks verify the in-memory session owner in `plugins/filter.py:821-933` and group pagination verifies the group session in `plugins/group_connect.py:317-365`.
- ObjectId parsing returns failure rather than propagating malformed input in central database methods such as `database/db.py:1181-1185` and `database/db.py:1612-1616`.
- The updater resolves a reviewed commit and stages/compiles source before applying it; the remaining atomicity issues are documented in H-09/L-06.
- The Docker image switches to a non-root `mccx` user at `Dockerfile:7-14`.

## Verification and repeat-pass ledger

### Automated/read-only verification

- Parsed all 26 project Python files with Python's AST parser: **0 syntax errors**.
- `ruff check .`: **passed** using the repository configuration.
- A broader Ruff security/async/bugbear scan was reviewed manually; it primarily identified swallowed-exception paths already represented above and no separate blocking-call finding beyond the catalog/duplicate/updater items.
- `pip-audit -r requirements.txt --progress-spinner off` with cache disabled: **no known vulnerabilities found** on 2026-08-27.
- Tracked secret-pattern scan across 41 files: **0 hits** for Telegram bot-token, private-key, credentialed MongoDB URI, and GitHub-token patterns.
- Relevant Git-history scan for `.env`, `*.session`, and `*.session-journal`: **0 commits found**.
- Current runtime artifact inspection: `.env` and session artifacts exist and are Git-ignored. Only schema/initialization presence was checked; secret values were not printed or copied.
- Test execution limitation: the repository's existing `.venv` lacks `rapidfuzz`, so pytest collection failed; the system interpreter lacks pytest. No packages or application files were modified to work around that environment issue.

### Review passes

The baseline discovery/security/performance pass produced the initial finding set. Independent repeat passes then continued until the required two consecutive passes produced no new issue:

| Pass | Focus | New issues |
|---|---|---:|
| Baseline | Entry points, dependency graph, all handlers, database design, complete indexing/search/delivery/update data flows | Initial set |
| Repeat 1 | AST task-lifecycle and sync-in-async scan; all `create_task` sites | 1 — unmanaged long-running/background task lifecycle (M-03) |
| Repeat 2 | Complete handler-to-delivery matrix; every media send/download and filesystem sink; callback/deep-link authorization | **0** |
| Repeat 3 | Tracked/history secret scan; Docker context; dependencies; CI/container configuration; test-coverage cross-check | **0** |

**Stopping condition reached:** Repeat 2 and Repeat 3 were consecutive zero-new-issue passes.

## Recommended remediation order

1. **Contain C-01 immediately:** exclude session/runtime state from Docker context, invalidate exposed images/layers, and rotate/revoke credentials if an image was distributed.
2. Centralize final delivery authorization (H-01), make gates tri-state/fail-closed for indeterminate results (H-02), and repair Request-FSub state/identifier handling (H-03).
3. Stop silent data loss/corruption: checkpoint only acknowledged batches (H-04), repair duplicate deletion (H-05), isolate registry write phases (H-06), and require uniqueness indexes (H-07).
4. Add public workload controls and replace scan-heavy search (H-08); then centralize FloodWait behavior (M-02).
5. Replace or harden the self-updater (H-09), supervise background jobs (M-03/M-04), and address remaining memory/latency risks.
6. Add security regression tests and a reproducible clean test environment before broader refactoring.

## Safest first change awaiting approval

The safest first change is a narrow non-application-code containment patch to `.dockerignore`: exclude `*.session`, `*.session-journal`, `runtime/`, `.deployed_sha`, and `.deployed_files.json`, plus a test that fails if any of those paths enter the Docker context. After that, rebuild clean images and decide whether session/token rotation is required based on whether an existing image was ever exported or published. No application code was modified during this audit.
