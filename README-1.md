<div align="center">

<img src="https://files.catbox.moe/e3b5ym.mp4" alt="MCCxBot Banner" width="600"/>

# 🎬 MCCxBot
### Malayalam Cinema Club — Auto Filter & File Delivery Bot

[![Python](https://img.shields.io/badge/Python-3.9%2B-blue?style=flat-square&logo=python)](https://python.org)
[![Pyrogram](https://img.shields.io/badge/Pyrogram-2.0%2B-green?style=flat-square)](https://pyrogram.org)
[![MongoDB](https://img.shields.io/badge/MongoDB-Atlas-brightgreen?style=flat-square&logo=mongodb)](https://mongodb.com)
[![License](https://img.shields.io/badge/License-MIT-yellow?style=flat-square)](LICENSE)
[![Telegram](https://img.shields.io/badge/Telegram-MCCxBot-blue?style=flat-square&logo=telegram)](https://t.me/MCCxUpdates)

*A production-grade Telegram bot serving the Malayalam Cinema Community — 1.5M+ files, multi-cluster MongoDB, TMDB integration, and a full admin suite.*

</div>

---

## ✨ Features

### 🔍 Search & Delivery
- **Smart auto-filter** — searches across 5 MongoDB clusters instantly
- **Language + Quality preset** — type `Malayalam 1080p Leo` to skip all selection steps
- **Spell correction** — suggests similar titles when no results found
- **Paginated results** — language → quality → file list with back navigation
- **Group search** — users search in connected groups, files delivered in PM
- **FSub gate** — force subscribe before access, with inline join buttons
- **Auto-delete** — files auto-delete after configurable time with 1-min warning

### 🎛 Admin Panel (`/admin`)
- **Analytics** — users, files, groups, cluster storage bars, language breakdown, top active groups — all in one screen
- **File Manager** — search, delete, rename, find duplicates, bulk delete by pattern, quick CAM purge, cluster migration, top missing files
- **Group Manager** — list all groups, ban/unban, per-group settings, whitelist/blacklist mode, top groups by activity
- **FSub Manager** — add/remove join channels, refresh invite links, channel health check
- **Maintenance Mode** — toggle with custom message, admin exempt
- **Caption Template** — custom file captions with `{filename}`, `{quality}`, `{lang}`, `{size}`, `{username}`, `{delete_minutes}` variables
- **Config Backup/Restore** — export settings as JSON, restore with protected fields
- **Channel Health Check** — verifies bot admin status in all configured channels

### 📢 Broadcast
- **Preview before send** — shows recipient count, time estimate, message preview
- **Target flags** — `-users`, `-groups`, or both
- **Schedule** — `-schedule 2h` or `-schedule 30m`
- **Auto-delete** — `-del` removes broadcast after 24 hours
- **Pin** — `-pin` silently pins for each user

### 📚 Indexing
- **Super-Indexer** — bulk index entire channels with pause/resume
- **Real-time indexer** — auto-indexes new uploads from configured DB channels
- **Post queue** — drains at 1 post/3s to prevent FloodWait
- **Smart log notifications** — completion and stop summaries to log channel

### 🔔 Background Tasks
- **Health monitor** — pings all clusters every 10 minutes, alerts on failure, green heartbeat every 6 hours
- **Birthday broadcast** — optional daily birthday greetings to users
- **Auto request fulfillment** — notifies users when a requested movie is indexed

### 🎬 Movie Requests
- **Ticket system** — users request movies, tickets sent to admin log channel
- **Auto-fulfillment** — when a requested title gets indexed, user is notified automatically

---

## 🗂 Project Structure

```
MCCxBot/
│
├── bot.py                    # Entry point
├── utils.py                  # FSub helpers
├── tmdb.py                   # TMDB API integration
├── requirements.txt
├── .env                      # ← Never commit this
│
├── database/
│   ├── __init__.py
│   └── db.py                 # Multi-cluster MongoDB layer with config cache
│
└── plugins/
    ├── admin.py              # Admin panel, commands
    ├── filter.py             # PM search, file delivery
    ├── start.py              # /start handler, deep links
    ├── welcome.py            # Group welcome messages
    ├── group_connect.py      # Group search handler
    ├── index.py              # Super-Indexer (bulk)
    ├── indexer.py            # Real-time indexer + post queue
    ├── request.py            # Movie request system
    ├── broadcast.py          # Broadcast command
    ├── file_manager.py       # File Manager panel
    ├── group_manager.py      # Group Manager panel
    ├── health_monitor.py     # Background health + birthday tasks
    └── state.py              # Shared admin input state
```

---

## ⚙️ Configuration

Copy `.env.example` to `.env` and fill in your values:

```env
# ── Telegram Credentials (Required) ──────────────────────────────
API_ID=                        # From https://my.telegram.org
API_HASH=                      # From https://my.telegram.org
BOT_TOKEN=                     # From @BotFather

# ── Admin ────────────────────────────────────────────────────────
ADMIN_ID=                      # Your Telegram user ID

# ── Channels ─────────────────────────────────────────────────────
LOG_CHANNEL_ID=                # Channel where logs are sent (bot must be admin)
DATABASE_CHANNEL_ID=           # Primary file storage channel
UPDATE_CHANNEL=                # Update channel ID (numeric)
UPDATE_CHANNEL_LINK=           # e.g. https://t.me/YourChannel
MAIN_GROUP_LINK=               # e.g. https://t.me/YourGroup

# ── APIs ─────────────────────────────────────────────────────────
TMDB_API_KEY=                  # From https://themoviedb.org/settings/api

# ── MongoDB (Required — up to 5 clusters) ────────────────────────
DATABASE_URI=                  # Primary cluster (mandatory)
DATABASE_URI_2=                # Additional clusters (optional)
DATABASE_URI_3=
DATABASE_URI_4=
DATABASE_URI_5=
```

> **Note:** All other settings (welcome text, FSub channels, auto-delete time, caption template, etc.) are managed live via `/admin` and stored in MongoDB. You do not need to restart the bot to change them.

---

## 🚀 Deployment

### Local / VPS

```bash
# 1. Clone the repo
git clone https://github.com/yourusername/MCCxBot.git
cd MCCxBot

# 2. Install dependencies
pip install -r requirements.txt

# 3. Set up environment
cp .env.example .env
# Edit .env with your values

# 4. Run
python bot.py
```

### Docker

```bash
docker build -t mccxbot .
docker run --env-file .env mccxbot
```

### Cloud (Render / Railway / Heroku / Mango)

1. Connect your GitHub repository
2. Set all environment variables in the platform's dashboard (do **not** upload `.env`)
3. Set the start command to `python bot.py`
4. Deploy

---

## 📋 Requirements

```
pyrogram>=2.0.0
TgCrypto
motor
pymongo
python-dotenv
requests
aiohttp
```

---

## 🤖 Bot Setup Checklist

After deploying, do these steps once:

- [ ] Add bot as **Admin** in your Log Channel (with post messages permission)
- [ ] Add bot as **Admin** in your Database Channel (with post messages permission)
- [ ] Add bot as **Admin** in any FSub channels
- [ ] Run `/admin` → **Channel Health Check** to verify all channels
- [ ] Run `/admin` → **Manage FSub** → add your channels
- [ ] Set your welcome text and media via `/admin`
- [ ] Forward a message from your DB channel to the bot → tap **Start Indexing** to index existing files

---

## 📸 Screenshots

| Start Screen | Search Results | Admin Panel |
|:---:|:---:|:---:|
| *(add screenshot)* | *(add screenshot)* | *(add screenshot)* |

---

## 🙏 Credits

**Built by:** [joe7](https://t.me/joe7) — Malayalam Cinema Club

**Powered by:**
- [Pyrogram](https://pyrogram.org) — Telegram MTProto framework
- [Motor](https://motor.readthedocs.io) — Async MongoDB driver
- [TMDB API](https://themoviedb.org) — Movie metadata

**Community:** [MCCxUpdates](https://t.me/MCCxUpdates) | [MCCxRequest](https://t.me/MCCxRequest)

---

## ⚠️ Disclaimer

This bot is built for the Malayalam Cinema Community for **personal and community use**. The developer is not responsible for how others use this software. Do not use this bot to distribute copyrighted content without proper authorization.

---

<div align="center">

Made with ❤️ for the Malayalam Cinema Community

⭐ Star this repo if it helped you!

</div>
