from __future__ import annotations

import argparse
import ast
import json
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable

SOURCE_ROOTS = (
    "hfa-core/src",
    "hfa-control/src",
    "hfa-worker/src",
    "hfa-tools/src",
    "hfa-agents/src",
    "hfa-semantic/src",
)

WRITE_OPERATION_PATTERN = re.compile(
    r"\b(?:set|setex|hset|zadd|sadd|delete|expire|xadd|eval|evalsha|"
    r"transition_state|set_task_state|mark_running|mark_completed|store_result|"
    r"task_admit|task_dispatch_commit|task_claim_start|task_complete|task_requeue)\b",
    re.IGNORECASE,
)

STATE_TOKEN_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("run_state", re.compile(r"RedisKey\.run_state|hfa:run(?::state)?:", re.IGNORECASE)),
    ("run_meta", re.compile(r"RedisKey\.run_meta|hfa:run:.*:meta", re.IGNORECASE)),
    ("dag_task_state", re.compile(r"DagRedisKey\.task_state|task_state_key", re.IGNORECASE)),
    ("dag_task_meta", re.compile(r"DagRedisKey\.task_meta|task_meta_key", re.IGNORECASE)),
    ("task_output", re.compile(r"DagRedisKey\.task_output|task_output_key", re.IGNORECASE)),
    ("queue", re.compile(r"ready_queue|scheduled_zset|running_zset|stream", re.IGNORECASE)),
    ("ownership", re.compile(r"claim|owner|reservation|lease|epoch", re.IGNORECASE)),
    ("audit", re.compile(r"event_store|audit|CanonicalTransitionRecord", re.IGNORECASE)),
)

ALLOWED_AUTHORITY_CLASSES = {
    "canonical_candidate",
    "execution_truth",
    "coordination_state",
    "projection",
    "compatibility",
    "transport",
    "audit",
    "unknown",
}

ALLOWED_REACHABILITY = {
    "production_default",
    "production_flagged",
    "test_only",
    "unreachable",
    "unknown",
}


@dataclass(frozen=True)
class WriterRecord:
    writer_id: str
    path: str
    line: int
    function: str
    state_family: str
    key_pattern: str
    operation: str
    write_semantics: str
    transaction_boundary: str
    guard: str
    feature_flags: tuple[str, ...]
    production_reachability: str
    authority_class: str
    redis_capability_source: str
    composition_root: str


def _function_ranges(source: str) -> list[tuple[int, int, str]]:
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return []
    ranges: list[tuple[int, int, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            ranges.append((node.lineno, getattr(node, "end_lineno", node.lineno), node.name))
    return ranges


def _function_for_line(ranges: Iterable[tuple[int, int, str]], line: int) -> str:
    matches = [(end - start, name) for start, end, name in ranges if start <= line <= end]
    return min(matches, default=(0, "<module>"))[1]


def _state_family(context: str, path: str) -> tuple[str, str]:
    for family, pattern in STATE_TOKEN_PATTERNS:
        match = pattern.search(context)
        if match:
            return family, match.group(0)
    if path.endswith(".lua") and "/lua/task_" in path:
        return "dag_task_state", "task lua state keys"
    return "unknown", "unknown"


def _operation(line: str) -> str:
    match = WRITE_OPERATION_PATTERN.search(line)
    return match.group(0) if match else "unknown"


def _authority_class(path: str, family: str) -> str:
    normalized = path.lower()
    if "/semantic/" in normalized or "semantic_bridge" in normalized or "feedback" in normalized:
        return "projection"
    if "event_store" in normalized or "/audit" in normalized:
        return "audit"
    if any(token in normalized for token in ("registry.py", "shard.py", "heartbeat.py", "reservation")):
        return "coordination_state"
    if "/hfa/lua/task_" in normalized or "dag_lua.py" in normalized or "task_recovery.py" in normalized:
        return "execution_truth"
    if "state_store.py" in normalized or "hfa_worker/consumer.py" in normalized:
        return "compatibility"
    if "hfa/state/" in normalized or "admission.py" in normalized:
        return "canonical_candidate"
    if family in {"queue", "ownership"}:
        return "coordination_state"
    return "unknown"


def _reachability(path: str) -> str:
    normalized = path.lower()
    if normalized.startswith("tests/"):
        return "test_only"
    if any(
        token in normalized
        for token in (
            "hfa-control/src/hfa_control/dag_lua.py",
            "hfa-core/src/hfa/lua/task_",
            "hfa-worker/src/hfa_worker/service.py",
            "hfa-worker/src/hfa_worker/task_consumer.py",
            "hfa-control/src/hfa_control/admission.py",
        )
    ):
        return "production_default"
    if any(token in normalized for token in ("worker_runtime.py", "semantic_bridge.py")):
        return "production_flagged"
    if any(token in normalized for token in ("state_store.py", "hfa_worker/consumer.py")):
        return "production_flagged"
    return "unknown"


def _feature_flags(source: str) -> tuple[str, ...]:
    return tuple(sorted(set(re.findall(r"\b(?:IRON|HFA)_[A-Z0-9_]+\b", source))))


def _redis_capability_source(path: str) -> str:
    normalized = path.lower()
    if normalized.endswith(".lua") or "dag_lua.py" in normalized:
        return "lua_gateway"
    if "state_store.py" in normalized or "consumer.py" in normalized:
        return "compatibility_facade"
    if any(token in normalized for token in ("repository.py", "store.py")):
        return "restricted_repository"
    return "injected_raw_client"


def _transaction_boundary(path: str, line: str) -> str:
    normalized = path.lower()
    if normalized.endswith(".lua"):
        return "single_redis_lua_call"
    if ".eval" in line or "evalsha" in line:
        return "single_redis_lua_call"
    return "multi_command_or_unknown"


def _guard(context: str) -> str:
    tokens = []
    for token in ("expected_state", "claim_epoch", "scheduler_epoch", "idempot", "NX", "XX"):
        if token.lower() in context.lower():
            tokens.append(token)
    return ",".join(tokens) if tokens else "none_observed"


def _write_semantics(path: str, family: str, operation: str) -> str:
    if family == "unknown":
        return "unclassified_write_candidate"
    if path.endswith(".lua"):
        return f"atomic_{family}_{operation.lower()}"
    return f"{family}_{operation.lower()}"


def _composition_root(path: str) -> str:
    normalized = path.lower()
    if "hfa-worker/src/hfa_worker" in normalized:
        return "WorkerService_or_worker_runtime"
    if "hfa-control/src/hfa_control" in normalized:
        return "ControlPlaneService_or_scheduler"
    if "hfa-core/src/hfa" in normalized:
        return "shared_runtime_library"
    return "unknown"


def _iter_source_files(repo_root: Path) -> Iterable[Path]:
    for root_text in SOURCE_ROOTS:
        root = repo_root / root_text
        if not root.exists():
            continue
        yield from root.rglob("*.py")
        yield from root.rglob("*.lua")


def scan_writer_inventory(repo_root: Path) -> list[WriterRecord]:
    repo_root = repo_root.resolve()
    records: list[WriterRecord] = []
    seen: set[tuple[str, int, str]] = set()

    for file_path in sorted(_iter_source_files(repo_root)):
        relative = file_path.relative_to(repo_root).as_posix()
        source = file_path.read_text(encoding="utf-8", errors="replace")
        lines = source.splitlines()
        ranges = _function_ranges(source) if file_path.suffix == ".py" else []
        flags = _feature_flags(source)

        for index, line in enumerate(lines, start=1):
            if not WRITE_OPERATION_PATTERN.search(line):
                continue
            start = max(0, index - 6)
            end = min(len(lines), index + 5)
            context = "\n".join(lines[start:end])
            family, key_pattern = _state_family(context, relative)
            if family == "unknown" and not any(
                token in relative.lower()
                for token in ("state", "consumer", "admission", "recovery", "lua", "audit")
            ):
                continue

            operation = _operation(line)
            identity = (relative, index, operation)
            if identity in seen:
                continue
            seen.add(identity)

            authority = _authority_class(relative, family)
            records.append(
                WriterRecord(
                    writer_id=f"{relative}:{index}:{operation}",
                    path=relative,
                    line=index,
                    function=_function_for_line(ranges, index),
                    state_family=family,
                    key_pattern=key_pattern,
                    operation=operation,
                    write_semantics=_write_semantics(relative, family, operation),
                    transaction_boundary=_transaction_boundary(relative, line),
                    guard=_guard(context),
                    feature_flags=flags,
                    production_reachability=_reachability(relative),
                    authority_class=authority,
                    redis_capability_source=_redis_capability_source(relative),
                    composition_root=_composition_root(relative),
                )
            )

    return records


def render_inventory(records: list[WriterRecord]) -> dict:
    unknown = [record.writer_id for record in records if record.authority_class == "unknown"]
    by_authority: dict[str, int] = {name: 0 for name in sorted(ALLOWED_AUTHORITY_CLASSES)}
    by_reachability: dict[str, int] = {name: 0 for name in sorted(ALLOWED_REACHABILITY)}
    for record in records:
        by_authority[record.authority_class] = by_authority.get(record.authority_class, 0) + 1
        by_reachability[record.production_reachability] = (
            by_reachability.get(record.production_reachability, 0) + 1
        )
    return {
        "schema_version": 1,
        "writer_count": len(records),
        "unknown_writer_count": len(unknown),
        "unknown_writer_ids": unknown,
        "authority_class_counts": by_authority,
        "production_reachability_counts": by_reachability,
        "writers": [asdict(record) for record in records],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Inventory Sprint 80 state writers")
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--out", default="local_out/sprint80/writer_inventory.json")
    args = parser.parse_args(argv)

    payload = render_inventory(scan_writer_inventory(Path(args.repo_root)))
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
