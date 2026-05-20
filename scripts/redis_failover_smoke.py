#!/usr/bin/env python3
"""Redis restart/failover smoke validation."""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import time
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass(frozen=True)
class Step:
    name: str
    status: str
    returncode: int
    stdout_tail: str = ""
    stderr_tail: str = ""


def _tail(value: str, limit: int = 2000) -> str:
    return value[-limit:]


def run_cmd(args: list[str], cwd: Path) -> Step:
    proc = subprocess.run(
        args,
        cwd=str(cwd),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    return Step(
        name=" ".join(args),
        status="PASS" if proc.returncode == 0 else "FAIL",
        returncode=proc.returncode,
        stdout_tail=_tail(proc.stdout),
        stderr_tail=_tail(proc.stderr),
    )


def compose(repo_root: Path, *args: str) -> list[str]:
    docker = shutil.which("docker")
    if docker is None:
        return []
    return [docker, "compose", "-f", str(repo_root / "docker-compose.yml"), *args]


def wait_for_ping(repo_root: Path, timeout_s: int = 30) -> Step:
    deadline = time.time() + timeout_s
    last = Step("redis ping", "FAIL", 1, stderr_tail="not attempted")

    while time.time() < deadline:
        cmd = compose(repo_root, "exec", "-T", "redis", "redis-cli", "ping")
        if not cmd:
            return Step("docker compose redis ping", "SKIPPED", 127, stderr_tail="docker executable not found")

        last = run_cmd(cmd, repo_root)
        if last.returncode == 0 and "PONG" in last.stdout_tail:
            return Step("redis ping", "PASS", 0, stdout_tail=last.stdout_tail, stderr_tail=last.stderr_tail)
        time.sleep(1)

    return Step("redis ping", "FAIL", last.returncode, stdout_tail=last.stdout_tail, stderr_tail=last.stderr_tail)


def _classify_failure(steps: list[Step]) -> str:
    combined = "\n".join(step.stderr_tail for step in steps)
    if "dockerDesktopLinuxEngine" in combined or "Internal Server Error" in combined:
        return "docker_engine_or_image_unavailable"
    if "unable to get image" in combined or "pull access denied" in combined:
        return "image_unavailable"
    if "port is already allocated" in combined or "Ports are not available" in combined:
        return "port_conflict"
    if any(step.name == "redis ping" and step.status == "FAIL" for step in steps):
        return "redis_healthcheck_failed"
    return "unknown"


def _result(status: str, steps: list[Step]) -> dict:
    result = {
        "source": "redis_failover_smoke",
        "mode": "docker-compose",
        "status": status,
        "checked": len(steps),
        "failed": sum(1 for step in steps if step.status == "FAIL"),
        "steps": [asdict(step) for step in steps],
        "notes": [
            "Validates Redis startup and restart recovery using the root minimal smoke compose.",
            "This is a restart smoke, not a full Redis Sentinel/cluster failover test.",
        ],
    }
    if status == "FAIL":
        result["failure_class"] = _classify_failure(steps)
    return result


def run(repo_root: Path) -> dict:
    if shutil.which("docker") is None:
        return {
            "source": "redis_failover_smoke",
            "mode": "docker-compose",
            "status": "SKIPPED",
            "reason": "docker executable not found",
            "steps": [],
        }

    steps: list[Step] = []

    for args in [
        compose(repo_root, "config"),
        compose(repo_root, "up", "-d", "redis"),
    ]:
        step = run_cmd(args, repo_root)
        steps.append(step)
        if step.status != "PASS":
            return _result("FAIL", steps)

    first_ping = wait_for_ping(repo_root)
    steps.append(first_ping)
    if first_ping.status != "PASS":
        return _result("FAIL", steps)

    restart = run_cmd(compose(repo_root, "restart", "redis"), repo_root)
    steps.append(restart)
    if restart.status != "PASS":
        return _result("FAIL", steps)

    second_ping = wait_for_ping(repo_root)
    steps.append(second_ping)

    return _result("PASS" if all(step.status == "PASS" for step in steps) else "FAIL", steps)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run Redis restart/failover smoke")
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--output", default="docs/dashboard/artifacts/latest_redis_failover_smoke.json")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)

    result = run(Path(args.repo_root).resolve())

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2, sort_keys=True), encoding="utf-8")

    if args.json:
        print(json.dumps(result, sort_keys=True))
    else:
        print(f"REDIS_FAILOVER_SMOKE_{result['status']}")

    return 0 if result["status"] in {"PASS", "SKIPPED"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
