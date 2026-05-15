#!/usr/bin/env python
"""Run the integrated FULL AUTO v3.2 smoke/regression set.

This is a deterministic CI wrapper around the known-good integrated smoke set.
It sets feature flags and delegates to pytest.  Redis mode defaults to ``auto``:
if 127.0.0.1:6389 is reachable it uses the existing Redis service; otherwise it
sets fakeredis-compatible environment markers so CI can still run repositories
whose fixtures honor ``USE_FAKE_REDIS``.
"""
from __future__ import annotations

import argparse
import os
import socket
import subprocess
import sys
from pathlib import Path

SMOKE_TESTS = (
    "tests/core/test_event_store_core.py",
    "tests/core/test_replay_engine_core.py",
    "tests/core/test_task_complete_cqrs_slice.py",
    "tests/core/test_completion_capture_required.py",
    "tests/core/test_replay_without_llm.py",
    "tests/core/test_scheduler_commit_sealing.py",
    "tests/core/test_scheduler_single_authority.py",
    "tests/core/test_scheduler_dispatch_event_hook.py",
    "tests/chaos/test_zombie_completion.py",
    "tests/integration/test_reconciliation.py",
    "tests/integration/test_recovery_modes.py",
    "tests/integration/test_quarantine.py",
    "tests/integration/test_runtime_recovery_guard.py",
    "tests/integration/test_replay_compare_enforcement.py",
    "tests/integration/test_phase5b_strict_mode.py",
    "tests/integration/test_phase6a_runtime_safety.py",
)

FEATURE_ENV = {
    "IRON_V3_EVENT_GATE": "1",
    "IRON_V3_COMPLETION_SLICE": "1",
    "IRON_V3_LLM_SEALING": "1",
    "IRON_V3_SCHEDULER_SEAL": "1",
    "IRON_V3_WORKER_EFFECT_HYBRID": "1",
    "IRON_V3_PROOF_ENFORCEMENT": "1",
    "IRON_SEMANTIC_GATE_MODE": "gate",
}

EXISTING_REDIS_ENV = {
    "USE_EXISTING_REDIS": "1",
    "USE_FAKE_REDIS": "0",
    "REDIS_URL": "redis://127.0.0.1:6389/0",
}

FAKEREDIS_ENV = {
    "USE_EXISTING_REDIS": "0",
    "USE_FAKE_REDIS": "1",
    "REDIS_URL": "fakeredis://local/0",
}


def build_command(extra_pytest_args: list[str] | None = None) -> list[str]:
    return [
        sys.executable,
        "-m",
        "pytest",
        *SMOKE_TESTS,
        "-q",
        "--tb=short",
        *(extra_pytest_args or []),
    ]


def missing_tests(repo_root: Path) -> list[str]:
    return [test for test in SMOKE_TESTS if not (repo_root / test).exists()]


def redis_available(host: str = "127.0.0.1", port: int = 6389, timeout: float = 0.25) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def redis_env_for_mode(mode: str) -> dict[str, str]:
    if mode == "existing":
        return dict(EXISTING_REDIS_ENV)
    if mode == "fakeredis":
        return dict(FAKEREDIS_ENV)
    if redis_available():
        return dict(EXISTING_REDIS_ENV)
    return dict(FAKEREDIS_ENV)


def build_env(*, redis_mode: str, no_redis_env: bool = False) -> dict[str, str]:
    env = os.environ.copy()
    for key, value in FEATURE_ENV.items():
        env.setdefault(key, value)
    if not no_redis_env:
        for key, value in redis_env_for_mode(redis_mode).items():
            env[key] = value
    return env


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run FULL AUTO v3.2 integrated smoke tests.")
    parser.add_argument("--repo-root", default=".", help="Repository root.")
    parser.add_argument("--dry-run", action="store_true", help="Print command and exit.")
    parser.add_argument(
        "--redis-mode",
        choices=("auto", "existing", "fakeredis"),
        default="auto",
        help="Redis environment mode for integration tests.",
    )
    parser.add_argument("--no-redis-env", action="store_true", help="Do not set Redis-related env vars.")
    parser.add_argument("pytest_args", nargs="*", help="Extra pytest args after --")
    args = parser.parse_args(argv)

    repo_root = Path(args.repo_root).resolve()
    missing = missing_tests(repo_root)
    if missing:
        print("Missing smoke tests:", file=sys.stderr)
        for path in missing:
            print(f"  {path}", file=sys.stderr)
        return 2

    env = build_env(redis_mode=args.redis_mode, no_redis_env=args.no_redis_env)
    command = build_command(args.pytest_args)
    print("FULL AUTO v3.2 smoke command:")
    print(" ".join(command))
    if not args.no_redis_env:
        print(f"Redis mode: {args.redis_mode} -> {env.get('REDIS_URL', '<unset>')}")

    if args.dry_run:
        return 0

    completed = subprocess.run(command, cwd=repo_root, env=env, check=False)
    return int(completed.returncode)


if __name__ == "__main__":
    raise SystemExit(main())
