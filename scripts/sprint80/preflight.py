from __future__ import annotations

import argparse
import json
import os
import platform
import subprocess
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable
from urllib.parse import urlparse

AUDIT_BASE_BRANCH = "baseline/local-import"
AUDIT_BASE_COMMIT = "2eca85b2d9b115ad4588641b020e98efdd570a2d"
DIAGNOSTIC_BRANCH_PREFIXES = ("sprint/80", "audit/sprint80")
ALLOWED_CHANGED_PREFIXES = (
    "scripts/sprint80/",
    "tests/diagnostics/sprint80/",
    "local_out/sprint80/",
    ".github/workflows/sprint80-diagnostics.yml",
)
REQUIRED_RUNTIME_FILES = (
    "hfa-core/src/hfa/state/__init__.py",
    "hfa-core/src/hfa/runtime/state_store.py",
    "hfa-control/src/hfa_control/admission.py",
    "hfa-control/src/hfa_control/dag_lua.py",
    "hfa-core/src/hfa/lua/task_complete.lua",
    "hfa-worker/src/hfa_worker/consumer.py",
    "hfa-worker/src/hfa_worker/task_consumer.py",
    "hfa-worker/src/hfa_worker/main.py",
)


class GitCommandError(RuntimeError):
    pass


@dataclass(frozen=True)
class PreflightResult:
    status: str
    mode: str
    repository_root: str
    current_branch: str
    current_head: str
    expected_base_branch: str
    expected_base_commit: str
    merge_base: str
    changed_paths_from_base: tuple[str, ...]
    worktree_entries: tuple[str, ...]
    unexpected_changed_paths: tuple[str, ...]
    missing_runtime_files: tuple[str, ...]
    redis_url: str
    redis_is_isolated: bool | None
    reason: str


def _run_git(repo_root: Path, *args: str) -> str:
    proc = subprocess.run(
        ["git", *args],
        cwd=repo_root,
        text=True,
        capture_output=True,
        check=False,
    )
    if proc.returncode != 0:
        raise GitCommandError(
            f"git {' '.join(args)} failed ({proc.returncode}): {proc.stderr.strip()}"
        )
    return proc.stdout.strip()


def _normalize_repo_path(path: str) -> str:
    normalized = path.replace("\\", "/")
    while normalized.startswith("./"):
        normalized = normalized[2:]
    return normalized


def _is_allowlisted(path: str, allowed_prefixes: Iterable[str]) -> bool:
    normalized = _normalize_repo_path(path)
    return any(normalized.startswith(prefix) for prefix in allowed_prefixes)


def _parse_status_path(entry: str) -> str:
    if len(entry) < 4:
        return entry.strip()
    path = entry[3:].strip()
    if " -> " in path:
        path = path.split(" -> ", 1)[1]
    return path.strip('"')


def redis_url_is_isolated(redis_url: str) -> bool:
    if not redis_url:
        return False
    parsed = urlparse(redis_url)
    host = (parsed.hostname or "").lower()
    port = parsed.port or 6379
    local_hosts = {"localhost", "127.0.0.1", "::1"}
    if host not in local_hosts:
        return False
    if port == 6379 and os.getenv("SPRINT80_ALLOW_REDIS_6379", "0") != "1":
        return False
    return True


def run_preflight(
    repo_root: Path,
    *,
    expected_base_branch: str = AUDIT_BASE_BRANCH,
    expected_base_commit: str = AUDIT_BASE_COMMIT,
    redis_url: str = "",
    allowed_changed_prefixes: Iterable[str] = ALLOWED_CHANGED_PREFIXES,
) -> PreflightResult:
    repo_root = repo_root.resolve()
    current_branch = _run_git(repo_root, "branch", "--show-current")
    current_head = _run_git(repo_root, "rev-parse", "HEAD")
    merge_base = _run_git(repo_root, "merge-base", "HEAD", expected_base_commit)

    changed_raw = _run_git(
        repo_root,
        "diff",
        "--name-only",
        f"{expected_base_commit}..HEAD",
    )
    changed_paths = tuple(path for path in changed_raw.splitlines() if path.strip())

    status_raw = _run_git(repo_root, "status", "--short")
    worktree_entries = tuple(line for line in status_raw.splitlines() if line.strip())
    worktree_paths = tuple(_parse_status_path(entry) for entry in worktree_entries)

    compared_paths = tuple(dict.fromkeys((*changed_paths, *worktree_paths)))
    unexpected = tuple(
        path
        for path in compared_paths
        if not _is_allowlisted(path, allowed_changed_prefixes)
    )
    missing = tuple(
        path for path in REQUIRED_RUNTIME_FILES if not (repo_root / path).is_file()
    )

    exact_overlay = (
        current_branch == expected_base_branch
        and current_head == expected_base_commit
        and not unexpected
    )
    diagnostic_branch = (
        current_branch.startswith(DIAGNOSTIC_BRANCH_PREFIXES)
        and merge_base == expected_base_commit
        and not unexpected
    )

    if exact_overlay:
        mode = "exact_baseline_overlay"
    elif diagnostic_branch:
        mode = "diagnostic_branch"
    else:
        mode = "unsupported"

    isolated = redis_url_is_isolated(redis_url) if redis_url else None
    reasons: list[str] = []
    if merge_base != expected_base_commit:
        reasons.append("audit_base_commit_is_not_merge_base")
    if unexpected:
        reasons.append("product_source_mutated")
    if missing:
        reasons.append("required_runtime_files_missing")
    if redis_url and not isolated:
        reasons.append("diagnostic_redis_is_not_isolated")
    if mode == "unsupported":
        reasons.append("branch_or_head_not_a_supported_audit_mode")

    status = "PASS" if not reasons else "BLOCKED"
    return PreflightResult(
        status=status,
        mode=mode,
        repository_root=str(repo_root),
        current_branch=current_branch,
        current_head=current_head,
        expected_base_branch=expected_base_branch,
        expected_base_commit=expected_base_commit,
        merge_base=merge_base,
        changed_paths_from_base=changed_paths,
        worktree_entries=worktree_entries,
        unexpected_changed_paths=unexpected,
        missing_runtime_files=missing,
        redis_url=redis_url,
        redis_is_isolated=isolated,
        reason=";".join(reasons),
    )


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def build_environment_manifest(result: PreflightResult) -> dict:
    return {
        "audit_base_branch": result.expected_base_branch,
        "audit_base_commit": result.expected_base_commit,
        "branch": result.current_branch,
        "head": result.current_head,
        "mode": result.mode,
        "python_version": platform.python_version(),
        "platform": platform.platform(),
        "redis_url": result.redis_url,
        "redis_is_isolated": result.redis_is_isolated,
        "environment_flags": {
            key: os.getenv(key, "")
            for key in (
                "HFA_WORKER_TASK_CONSUMER_BRIDGE",
                "IRON_V3_WORKER_EFFECT_HYBRID",
                "IRON_V3_EVENT_GATE",
                "IRON_V3_PROOF_ENFORCEMENT",
                "HFA_ALLOW_LEGACY_DIRECT_TASK_CLAIM",
                "HFA_ALLOW_LEGACY_INJECTED_DISPATCH",
            )
        },
        "changed_paths_from_base": list(result.changed_paths_from_base),
        "worktree_entries": list(result.worktree_entries),
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Sprint 80 non-production audit preflight")
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--expected-base-branch", default=AUDIT_BASE_BRANCH)
    parser.add_argument("--expected-base-commit", default=AUDIT_BASE_COMMIT)
    parser.add_argument("--redis-url", default=os.getenv("SPRINT80_REDIS_URL", ""))
    parser.add_argument("--out", default="local_out/sprint80/preflight.json")
    parser.add_argument(
        "--environment-out",
        default="local_out/sprint80/environment.json",
    )
    args = parser.parse_args(argv)

    result = run_preflight(
        Path(args.repo_root),
        expected_base_branch=args.expected_base_branch,
        expected_base_commit=args.expected_base_commit,
        redis_url=args.redis_url,
    )
    write_json(Path(args.out), asdict(result))
    write_json(Path(args.environment_out), build_environment_manifest(result))
    print(json.dumps(asdict(result), indent=2, sort_keys=True))
    return 0 if result.status == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
