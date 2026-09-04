# Automatic Library

New files sent to your Telegram storage channel can enter Movie Bot without a
manual scan. Movie Bot watches **your destination channel**, not your friend's
database. Transfer Bot does not need to change.

## Safe setup

1. Add Movie Bot as an administrator in the destination storage channel.
2. Open **Control Center → Library → Source Channels**. Add that channel's ID
   or public link. Starting a manual index alone does not add a live source.
3. Open **Preferences → New Releases Channel** and configure the announcement
   channel. The bot needs permission to post there.
4. Configure `TMDB_API_KEY` (v3 API key) or `TMDB_BEARER_TOKEN` (API Read Access Token) for posters and descriptions. If both are set, the access token takes precedence. Keep these values only in your private hosting configuration or ignored `.env` file.
5. Open **Automatic Library → Check access**. Send a new test file to your
   storage channel; confirm it appears in search without manual indexing.
6. Open **Previews & review**. Check the poster and its **View Available Files**
   button privately, including your usual verification flow.
7. Only then select **Enable posts**. Public posts start paused by default.

Deployment needs the existing `OPERATIONS_DATABASE_URI`. Redis is still
optional in the normal all-in-one deployment. Do not deploy/restart in the
middle of an active bulk index without arranging a safe handover first.

## What gets posted

- One post per confirmed movie, including its release year.
- One post per series season. Episode titles do not create separate posts.
- Multiple sizes, languages and qualities stay available as separate files.
- Wait 90 seconds for variants to arrive, up to five minutes initially.
- Later uploads quietly edit the existing post, at most once per five minutes.
- Episode lists show actual availability, not an assumed complete season.
- Ratings are labelled **TMDB**, not IMDb.

Metadata ambiguity never prevents indexing. It holds only the announcement for
review. Use `/release_match candidate_id tmdb_id` from the review screen. If a
season was missing, append its number. The match is validated against TMDB
before it is applied. Related pending variants share the explicit choice.

## Recovery and visibility

The status screen shows received/indexed receipts, queued work, retries and
posts needing attention. Indexing continues when public posts are paused.
Database outages/full shards retain pending receipts instead of dropping them.

Receipts and checkpoints survive restarts. Recovery fetches known message IDs
in small batches and does not advance past unfinished files. Unknown older
history is not automatically scanned: the first observed message establishes
the activation baseline. Later known gaps are recovered silently, without
announcing old files. A new channel update establishes the latest observed
endpoint after downtime. A checkpoint is not proof that no newer unseen upload
exists; no general channel-history API is used with a bot token.

If Telegram times out during the first send, the outcome can be unknown. The
bot does **not** automatically resend. If the post exists, link its message ID
using `/release_link release_id message_id`; the release button is checked
before linking. Explicitly rejected sends can be retried after fixing access
with `/release_retry release_id`. Deleted/uneditable posts are flagged instead
of silently replaced. `/release_preview release_id` previews an individual
release privately.

Changing the New Releases destination pauses public posts and requires another
private preview. Old posts and old legacy outbox entries are not deleted or
replayed. Posts made before this feature have no permanent identity ledger;
the new one-post policy applies to releases tracked by this feature.

## Implementation and checks

`live_sources` stores activation/frontier/checkpoint information. `live_receipts`
stores one receipt per channel/message, with the media payload removed after
confirmed processing. `release_candidates` holds matching/review work.
`release_posts` retains the permanent movie/season post identity and message ID.
`release_files` holds unique file membership and deterministic ordering for
exact-release, cursor-paginated download links. These collections live only in
the Operations database. They have no automatic destructive cleanup; monitor
operations storage growth as the library grows.

Workers use fenced MongoDB claims. The all-in-one role starts both workers;
split deployments run live ingestion in `worker-indexer` and metadata/posts in
`worker-maintenance`. The interactive role captures receipts before normal
readiness/ephemeral-deduplication checks. Existing access checks remain on file
delivery.

Run `python tools/quality.py --skip-install` after installing development
requirements. For real MongoDB integration coverage, set `MCCX_TEST_MONGO_URI`
to an isolated loopback MongoDB before running tests. Tests create uniquely
named disposable databases and never use the application database for test
writes. `python tools/live_library_status.py --check-telegram` is a read-only
diagnostic using the local `.env`; it does not restart the bot or change settings.
