#!/usr/bin/env python3
"""Bounded Sprint 84.6 terminal-event evidence prepare/backfill tool."""
from __future__ import annotations

import argparse
import asyncio
import json
import time

import redis.asyncio as redis

from hfa_control.run_terminal_event_evidence import (
    backfill_terminal_event_evidence,
    ensure_terminal_event_index,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--redis-url", default="redis://127.0.0.1:6389/0")
    parser.add_argument("--page-size", type=int, default=500)
    parser.add_argument(
        "--prepare-only",
        action="store_true",
        help="Create/validate the persistent producer-contract HASH without setting migration readiness.",
    )
    return parser


async def _main() -> int:
    args = _parser().parse_args()
    client = redis.from_url(args.redis_url, decode_responses=True)
    try:
        if args.prepare_only:
            status = await ensure_terminal_event_index(client)
            print(json.dumps({"status": status}, sort_keys=True, separators=(",", ":")))
            return 0
        result = await backfill_terminal_event_evidence(
            client,
            page_size=args.page_size,
            completed_at_ms=int(time.time() * 1000),
        )
        print(json.dumps(result.__dict__, sort_keys=True, separators=(",", ":")))
        return 0
    finally:
        await client.aclose()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
