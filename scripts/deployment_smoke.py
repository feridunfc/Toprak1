#!/usr/bin/env python3
"""Deployment smoke validation for compose manifests.

This script is intentionally read-only. It validates that known compose files
can be parsed by `docker compose config` and writes a small dashboard artifact.
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass(frozen=True)
class ComposeCheck:
    path: str
    status: str
    returncode: int
    stdout_tail: str = ""
    stderr_tail: str = ""


def _tail(value: str, limit: int = 2000) -> str:
    return value[-limit:]


def check_compose(path: Path) -> ComposeCheck:
    if not path.exists():
        return ComposeCheck(str(path), "MISSING", 127, stderr_tail="compose file missing")

    docker = shutil.which("docker")
    if docker is None:
        return ComposeCheck(str(path), "SKIPPED", 127, stderr_tail="docker executable not found")

    proc = subprocess.run(
        [docker, "compose", "-f", str(path), "config"],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )

    return ComposeCheck(
        path=str(path),
        status="PASS" if proc.returncode == 0 else "FAIL",
        returncode=proc.returncode,
        stdout_tail=_tail(proc.stdout),
        stderr_tail=_tail(proc.stderr),
    )


def run(repo_root: Path) -> dict:
    compose_paths = [
        repo_root / "docker-compose.yml",
        repo_root / "docker-compose.integration.yml",
        repo_root / "docs" / "docker-compose.yml",
    ]
    checks = [check_compose(path) for path in compose_paths]
    failed = [check for check in checks if check.status == "FAIL" or check.status == "MISSING"]
    skipped = [check for check in checks if check.status == "SKIPPED"]
    return {
        "source": "deployment_smoke",
        "mode": "read-only",
        "status": "PASS" if not failed else "FAIL",
        "checked": len(checks),
        "failed": len(failed),
        "skipped": len(skipped),
        "checks": [asdict(check) for check in checks],
        "notes": [
            "Root docker-compose.yml is the minimal Redis smoke manifest.",
            "docker-compose.integration.yml is Redis integration support.",
            "docs/docker-compose.yml is a reference topology and not currently build-ready.",
        ],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate deployment compose manifests")
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--output", default="docs/dashboard/artifacts/latest_deployment_smoke.json")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)

    result = run(Path(args.repo_root))
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2, sort_keys=True), encoding="utf-8")

    if args.json:
        print(json.dumps(result, sort_keys=True))
    else:
        print(f"DEPLOYMENT_SMOKE_{result['status']}: checked={result['checked']} failed={result['failed']} skipped={result['skipped']}")

    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
