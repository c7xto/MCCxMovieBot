<div align="center">

# 🎬 MCCx Movie Hub

Fast Telegram movie and series search for the Malayalam Cinema Club.

**Kurigram · PyMongo Async · MongoDB · TMDB · Docker**

</div>

## What it does

- Searches movie and series files across as many as five MongoDB clusters.
- Understands title, language and quality terms such as `Leo Malayalam 1080p`.
- Delivers cached Telegram files privately with durable auto-deletion.
- Supports group-to-private search handoff, spell suggestions and series grouping.
- Indexes storage channels in real time or through the resumable bulk indexer.
- Provides a Telegram-native control center for files, groups, channels, access gates, analytics and health.
- Tracks missing requests and automatically notifies users when matching files arrive.

Premium plans and web streaming are deliberately outside this project's current scope.

## Reliability design

- `file_registry` provides cross-cluster `file_id` uniqueness and is kept in sync by every delete, purge and reset path.
- Bulk writes distinguish complete success, partial success, duplicates and genuine retryable failures.
- The primary MongoDB cluster is required at startup; optional clusters degrade independently.
- Search sessions are bounded and expire automatically.
- Message deletions use a persistent MongoDB queue, so restarts do not cancel them.
- Background task crashes and cluster failures are reported to the configured log channel.
- The self-updater stages and compiles the complete target commit before applying it, with rollback on an apply failure.

## Requirements

- Python 3.13 recommended
- Telegram `API_ID`, `API_HASH` and bot token
- MongoDB connection URI
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
docker compose logs -f bot
```

## Required environment variables

| Variable | Purpose |
|---|---|
| `API_ID` | Telegram application ID |
| `API_HASH` | Telegram application hash |
| `BOT_TOKEN` | BotFather token |
| `ADMIN_ID` | One or more comma-separated numeric admin IDs |
| `DATABASE_URI` | Required primary MongoDB cluster |

`DATABASE_URI_2` through `DATABASE_URI_5` are optional. Channel IDs, community links and the TMDB key are documented in `.env.example`; most presentation and access settings can then be managed live through `/admin`.

## Main commands

| Command | Who | Purpose |
|---|---|---|
| `/start` | Everyone | Open the Movie Hub |
| `/help` | Everyone | Search guide |
| `/request <title>` | Everyone | Request a missing title |
| `/admin` | Admin | Open the control center |
| `/stats` | Admin | View operational statistics |
| `/broadcast` | Admin | Preview and send a broadcast |
| `/filesearch <query>` | Admin | Find and manage indexed files |
| `/update <commit-sha>` | Admin | Review and apply a pinned update |
| `/cancel` | Admin | Cancel the current admin input flow |

## First deployment checklist

1. Add the bot as an administrator in the log and database channels.
2. Open `/admin` → **System** → **Channel Check**.
3. Configure source and required-subscription channels.
4. Forward a storage-channel message to the bot to begin bulk indexing.
5. Run `python tools/migrate_registry.py` once if upgrading an older database.

## Development and verification

```bash
pip install -r requirements-dev.txt
python -m compileall -q .
ruff check .
pytest -q
pip-audit -r requirements.txt --progress-spinner off
```

The GitHub Actions workflow runs these checks on every push and pull request.

## Important operational notes

- Search still uses MongoDB regex traversal. For very large libraries, the next major performance project should be Atlas Search or an equivalent text index with measured relevance and latency.
- Scheduled broadcasts are kept in process and are cancelled by a restart; ordinary delivered-file deletion is durable.
- Use only media you are legally authorized to distribute and follow Telegram and hosting-provider rules.

## License

[MIT](LICENSE)
