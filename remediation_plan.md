# Security and Architecture Remediation Plan

Items are completed strictly in severity order. A checkbox is marked only after the isolated fix passes `ruff check .` and syntax verification for every modified Python file.

## Critical

- [x] C-01 — Live Pyrogram session credentials enter the Docker image

## High

- [x] H-01 — Ban and maintenance controls are bypassable through delivery and deep-link paths
- [x] H-02 — Verification gates fail open when Telegram or MongoDB is unavailable
- [x] H-03 — Request-FSub can be bypassed by ignoring one prompt, and private links cannot be verified
- [x] H-04 — Bulk indexing permanently skips a failed database batch
- [x] H-05 — Per-group duplicate deletion removes every copy, including the kept copy
- [x] H-06 — Registry metadata failure can orphan a successful physical insert
- [x] H-07 — Required uniqueness indexes are optional at startup
- [x] H-08 — Public searches and file sends lack workload controls
- [x] H-09 — Self-update is not atomic across code, dependencies, callbacks, or concurrent runs

## Medium

- [x] M-01 — Full-catalog and duplicate operations block the event loop and can consume large memory
- [x] M-02 — FloodWait handling is inconsistent on core paths
- [x] M-03 — Long-running and fire-and-forget tasks are not lifecycle-managed
- [x] M-04 — Realtime announcement work has an unbounded queue and task fan-out
- [x] M-05 — File retrieval ignores registry location and can accumulate 30-second shard waits
- [x] M-06 — Admin configuration restore buffers and parses an unbounded document
- [x] M-07 — Remote MongoDB transport encryption is optional
- [x] M-08 — Two-stage verification success is never cached
- [x] M-09 — Durable auto-delete jobs are discarded after three generic failures
- [x] M-10 — Group broadcasts materialize recipients and scheduled broadcasts are volatile
- [x] M-11 — Private invite links are written to logs and incompletely removed from config exports
- [x] M-12 — Security-critical behavior lacks executable regression coverage in the audited environment

## Low

- [x] L-01 — User-controlled Markdown and raw exceptions can spoof messages or leak details
- [x] L-02 — The process lock uses a predictable, symlink-following temporary path
- [x] L-03 — TMDB credentials are placed in the URL and connections are not pooled
- [x] L-04 — Container and CI supply-chain hardening is incomplete
- [x] L-05 — Cache invalidation and stale-index monitoring create avoidable churn/noise
- [x] L-06 — The updater's lexical path check does not defend against local symlink/manifest tampering
