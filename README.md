<div align="center">

# 🎬 MCCx Movie Hub

Fast Telegram movie and series search for the Malayalam Cinema Club.

**Kurigram · PyMongo Async · MongoDB · Redis · TMDB · Docker**

</div>

## What it does

- Searches movie and series files across as many as five MongoDB clusters.
- Understands title, language and quality terms such as `Leo Malayalam 1080p`.
- Delivers cached Telegram files privately with durable auto-deletion.
- Supports group-to-private search handoff, spell suggestions and series grouping.
- Indexes storage channels in real time or through the resumable bulk indexer.
- Shows live indexing speed, ETA and separate scanned, saved, existing and skipped counters.
- Scans the existing library for exact and probable duplicates without deleting anything.
- Provides a Telegram-native control center for files, groups, channels, access gates, analytics and health.
- Tracks missing requests and automatically notifies users when matching files arrive.

Premium plans and web streaming are deliberately outside this project's current scope.

## Reliability design

- `file_registry` prevents duplicate Telegram file IDs across clusters. New files also use Telegram's stable content identity when available.
- Bulk writes distinguish complete success, partial success, duplicates and genuine retryable failures.
- Live searches and file delivery take priority over each bounded indexer batch without starving background work.
- Every Telegram button is acknowledged before database or membership checks, preventing stale callback errors.
- Access gates use a TTL-backed verification cache and a short grace window for temporary Telegram failures.
- A required dedicated operations database keeps configuration, checkpoints and the cross-cluster registry away from movie-shard capacity limits.
- Redis shares short-lived sessions, cooldowns, callback deduplication, shard health and workload admission across replicas.
- The primary MongoDB cluster is required at startup; optional clusters degrade independently.
- Failed optional shards are removed from search fan-out immediately and restored by live health probes.
- Analytics is split into fast Overview, Library, Activity and Health pages; the Health page performs fresh shard checks.
- Search sessions are bounded and expire automatically.
- Message deletions use a persistent MongoDB queue, so restarts do not cancel them.
- Broadcasts show live speed and ETA, support pause/resume/safe stop, and resume from durable recipient checkpoints after restarts.
- Background task crashes and cluster failures are reported to the configured log channel.
- The self-updater stages and compiles the complete target commit before applying it, with rollback on an apply failure.

## Requirements

- Python 3.13 recommended
- Telegram `API_ID`, `API_HASH` and bot token
- MongoDB connection URI
- A separate Operations MongoDB URI
- Redis 5 or newer
- A numeric Telegram administrator ID

Runtime versions are pinned in `requirements.txt`. The project uses Kurigram as the maintained Pyrogram-compatible Telegram client and PyMongo's native async API instead of deprecated Motor.

## Local setup

```bash
git clone https://github.com/c7xto/MCCxMovieBot.git
cd MCCxMovieBot
python -m venv .venv
```

Activate the environment and install dependencies:

```bash
# Linux/macOS
source .venv/bin/activate

# Windows PowerShell
.venv\Scripts\Activate.ps1

pip install -r requirements.txt
```

Copy `.env.example` to `.env`, fill the required values, then run `python bot.py`.
The bot stops immediately with an actionable error when a required setting or the primary database is unavailable.

## Docker

```bash
cp .env.example .env
# Edit .env
docker compose up -d --build
docker compose logs -f bot-interactive worker-indexer worker-broadcast worker-maintenance
```

## Required environment variables

| Variable | Purpose |
|---|---|
| `API_ID` | Telegram application ID |
| `API_HASH` | Telegram application hash |
| `BOT_TOKEN` | BotFather token |
| `ADMIN_ID` | One or more comma-separated numeric admin IDs |
| `DATABASE_URI` | Required primary MongoDB cluster |
| `OPERATIONS_DATABASE_URI` | Required dedicated database for settings, users, registry, counters and durable jobs |
| `REDIS_URL` | Required Redis endpoint for shared ephemeral state and coordination |
| `SERVICE_ROLE` | `all-in-one` on a one-process panel; Compose assigns dedicated roles |

`DATABASE_URI_2` through `DATABASE_URI_5` are optional. Channel IDs,
community links and the TMDB API Read Access Token (`TMDB_BEARER_TOKEN`) are
documented in `.env.example`; most presentation and access settings can then
be managed live through `/admin`.

When `OPERATIONS_DATABASE_URI` is first added, startup copies operational data in
resumable batches, validates the copy, and leaves the old data untouched. Do
not point it at a movie shard if you want true storage isolation.

All non-loopback MongoDB connections must use certificate-validated TLS. Prefer
`mongodb+srv://...`; a standard `mongodb://...` remote URI must include
`tls=true`. The bundled public CA store is used by default, or set
`MONGODB_TLS_CA_FILE` to a readable private CA bundle. Plaintext MongoDB is
accepted only for loopback development endpoints. The
`ALLOW_INSECURE_MONGODB_FOR_DEVELOPMENT=true` override bypasses this startup
check and must never be enabled against production data.

Ordinary configuration backups redact all private Telegram invite links. To
make a deliberate secret-bearing backup, set a 16+ character
`CONFIG_EXPORT_PASSPHRASE` and use **Encrypted Secret Backup** in `/admin`.
The result uses scrypt and AES-256-GCM. Decrypt it offline with
`python tools/decrypt_config_backup.py BACKUP OUTPUT`; the tool refuses to
overwrite an existing plaintext file.

## Main commands

| Command | Who | Purpose |
|---|---|---|
| `/start` | Everyone | Open the Movie Hub |
| `/help` | Everyone | Search guide |
| `/request <title>` | Everyone | Request a missing title |
| `/admin` | Admin | Open the control center |
| `/stats` | Admin | View operational statistics |
| `/broadcast` | Admin | Preview and send a broadcast |
| `/broadcast_status` | Admin | View, pause, resume or safely stop broadcasts |
| `/filesearch <query>` | Admin | Find and manage indexed files |
| `/update <commit-sha>` | Admin | Review and apply a pinned update |
| `/cancel` | Admin | Cancel the current admin input flow |

## First deployment checklist

1. Add the bot as an administrator in the log and database channels.
2. Open `/admin` → **System** → **Telegram Channels**.
3. Configure source and required-subscription channels.
4. Forward a storage-channel message to the bot to begin bulk indexing.
5. Run `python tools/migrate_registry.py` once if upgrading an older database.
6. Run the same migration after attaching a database that already contains movie rows. It is additive and safe to re-run.
7. Run `python tools/migrate_search_tokens.py --apply` to backfill strict search tokens and initialize language counters.

## Development and verification

```bash
python tools/quality.py
```

This one command installs the exact pinned development requirements, compiles
the source, runs Ruff and the complete test suite, and audits runtime
dependencies. The GitHub Actions workflow runs the same gate in a clean Python
3.13 environment on every push and pull request.

## Important operational notes

- User search uses indexed title tokens only, with bounded RapidFuzz typo correction. Legacy rows must be backfilled with `python tools/migrate_search_tokens.py --apply`; filename regex scans are not used as a fallback.
- Slash commands are never treated as movie searches and never enter missing-search analytics.
- `tools/migrate_registry.py` now enriches legacy rows with stable Telegram identities as well as file IDs. It requires every configured shard to be online so it cannot produce a misleading partial registry.
- Duplicate scans start in report-only mode. Verified exact copies can be removed only after a separate admin confirmation; probable matches are never auto-deleted, and language, quality, codec, size, season and episode variants are preserved.
- Broadcast source messages must remain available in the administrator chat until a scheduled job completes.
- Use only media you are legally authorized to distribute and follow Telegram and hosting-provider rules.

## License

[MIT](LICENSE)
