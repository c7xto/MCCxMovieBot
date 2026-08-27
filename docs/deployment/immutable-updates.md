# Immutable update procedure

Container replacement remains the strongest production deployment method. The
bot also provides a commit-pinned `/update COMMIT_SHA` command for hosts such as
Optiklink where rebuilding an image is not convenient.

## Build and verify

1. Check out the exact reviewed full commit SHA on a trusted build host.
2. Run the repository test and lint commands.
3. Build the image with a tag containing that full SHA:

   ```sh
   docker build --pull -t mccxmoviebot:<full-commit-sha> .
   ```

4. If using a registry, push the image and record its content digest. Production
   should reference `repository@sha256:<digest>` so a tag cannot be replaced.

## Roll out

Set `BOT_IMAGE` to the reviewed SHA tag or registry digest, then recreate only
the bot service:

```sh
docker compose pull bot
docker compose up -d --no-deps bot
```

The named `bot-runtime` volume preserves the Pyrogram session while the image
contains code and dependencies together. Do not mount the project source or a
virtual environment into `/app`.

## Roll back

Restore `BOT_IMAGE` to the previously recorded digest and repeat the two rollout
commands. Keep at least one known-good prior digest until the new release has
passed health checks.

## Optiklink and similar panel hosts

1. Push the tested release to the protected `main` branch.
2. Copy its full commit SHA.
3. Send `/update COMMIT_SHA` to the bot in a private chat.
4. Open the shown GitHub changes and confirm the update.

The updater accepts only commits already contained in `main`. It stages and
compiles the complete release before changing live files, rejects path escapes
and symlinks, installs dependencies only from the hash-locked file, keeps a
rollback copy, prevents concurrent updates, and protects `.env`, sessions, and
runtime data.
