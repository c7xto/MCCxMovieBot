"""Measure Redis-backed search latency against configured production-like data.

This tool is read-only. Start with a staging database and increase concurrency
gradually; a 100,000-request run is not equivalent to 100,000 simultaneous
Telegram sockets, but it provides a reproducible query-path capacity signal.
"""

import argparse
import asyncio
import json
import sys
import statistics
import time
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from database.db import db
from database.redis_client import redis_state


def percentile(values, percentile_value):
    if not values:
        return 0.0
    ordered = sorted(values)
    position = min(len(ordered) - 1, int((percentile_value / 100) * len(ordered)))
    return ordered[position]


async def run_load_test(queries, request_count, concurrency, target_ms):
    await redis_state.start()
    await asyncio.gather(*(database.command("ping") for database in db.dbs))
    await db.operations_db.command("ping")

    queue = asyncio.Queue()
    for index in range(request_count):
        queue.put_nowait(queries[index % len(queries)])

    latencies = []
    errors = []

    async def worker():
        while True:
            try:
                query = queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            started = time.perf_counter()
            try:
                await db.get_search_results(query, max_results=40)
            except Exception as error:
                errors.append(type(error).__name__)
            finally:
                latencies.append((time.perf_counter() - started) * 1000)
                queue.task_done()

    started = time.perf_counter()
    await asyncio.gather(*(worker() for _ in range(max(1, concurrency))))
    elapsed = time.perf_counter() - started
    report = {
        "requests": request_count,
        "concurrency": concurrency,
        "elapsed_seconds": round(elapsed, 3),
        "throughput_rps": round(request_count / elapsed, 2) if elapsed else 0,
        "latency_ms": {
            "mean": round(statistics.fmean(latencies), 2) if latencies else 0,
            "p50": round(percentile(latencies, 50), 2),
            "p95": round(percentile(latencies, 95), 2),
            "p99": round(percentile(latencies, 99), 2),
            "max": round(max(latencies), 2) if latencies else 0,
        },
        "errors": len(errors),
        "error_types": sorted(set(errors)),
        "target_ms": target_ms,
    }
    report["target_met"] = report["latency_ms"]["p95"] <= target_ms and not errors
    return report


async def main(args):
    queries = [query.strip() for query in args.queries.split(",") if query.strip()]
    if not queries:
        raise ValueError("At least one non-empty query is required")
    try:
        report = await run_load_test(
            queries,
            max(1, args.requests),
            max(1, args.concurrency),
            max(1.0, args.target_ms),
        )
        print(json.dumps(report, indent=2))
        return 0 if report["target_met"] else 1
    finally:
        await db.close()
        await redis_state.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--requests", type=int, default=1000)
    parser.add_argument("--concurrency", type=int, default=100)
    parser.add_argument("--target-ms", type=float, default=200.0)
    parser.add_argument(
        "--queries",
        default="aavesham 2024,reacher 2022,kgf 2,war machine",
    )
    raise SystemExit(asyncio.run(main(parser.parse_args())))
