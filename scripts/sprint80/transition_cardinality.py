from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from collections import Counter
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable

AUDIT_BASE_COMMIT = "2eca85b2d9b115ad4588641b020e98efdd570a2d"

PRIMARY_RECORD_CLASSES = {
    "TransportMessage",
    "RuntimeTransitionNotification",
    "EffectResultMessage",
    "AuditEvent",
    "EvidenceArtifact",
    "CanonicalTransitionRecord",
    "Unknown",
}

CANONICAL_TRANSITION_REQUIRED_FIELDS = {
    "aggregate_id",
    "aggregate_type",
    "from_revision",
    "to_revision",
    "command_id",
    "transition_type",
    "actor",
    "occurred_at",
    "payload_hash",
}


@dataclass(frozen=True)
class ObservedRecord:
    source: str
    source_key: str
    event_type: str
    primary_class: str
    fields: tuple[str, ...]
    canonical_schema_match: bool


@dataclass(frozen=True)
class OperationObservation:
    operation: str
    transaction_boundary: str
    state_before: str
    state_after: str
    aggregate_revision_candidate: int
    execution_lifecycle_mutation: int
    coordination_mutation: int
    projection_mutation: int
    transport_append: int
    transport_ack: int
    audit_append: int
    canonical_transition_record_count: int
    changed_keys: tuple[str, ...]
    records: tuple[ObservedRecord, ...]
    notes: tuple[str, ...] = ()


def decode(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def decode_mapping(mapping: dict[Any, Any]) -> dict[str, str]:
    return {decode(key): decode(value) for key, value in (mapping or {}).items()}


def matches_canonical_transition_schema(fields: Iterable[str]) -> bool:
    field_set = set(fields)
    receipt_present = "idempotency_key" in field_set or "receipt_reference" in field_set
    return CANONICAL_TRANSITION_REQUIRED_FIELDS <= field_set and receipt_present


def classify_record(*, source: str, source_key: str, fields: dict[str, str]) -> ObservedRecord:
    field_names = set(fields)
    event_type = fields.get("event_type", "")
    canonical = matches_canonical_transition_schema(field_names)

    if canonical:
        primary_class = "CanonicalTransitionRecord"
    elif source == "event_store":
        primary_class = "AuditEvent"
    elif event_type == "TaskRequested":
        primary_class = "TransportMessage"
    elif event_type in {"TaskScheduled", "TaskRequeued", "TaskFailed"}:
        primary_class = "RuntimeTransitionNotification"
    elif event_type in {"RunCompleted", "RunFailed", "EffectCommitted", "EffectFailed"}:
        primary_class = "EffectResultMessage"
    elif source == "evidence_artifact":
        primary_class = "EvidenceArtifact"
    else:
        primary_class = "Unknown"

    return ObservedRecord(
        source=source,
        source_key=source_key,
        event_type=event_type,
        primary_class=primary_class,
        fields=tuple(sorted(field_names)),
        canonical_schema_match=canonical,
    )


def render_report(observations: Iterable[OperationObservation]) -> dict[str, Any]:
    rows = list(observations)
    class_counts: Counter[str] = Counter()
    canonical_matches: list[dict[str, str]] = []

    for observation in rows:
        for record in observation.records:
            class_counts[record.primary_class] += 1
            if record.canonical_schema_match:
                canonical_matches.append(
                    {
                        "operation": observation.operation,
                        "source_key": record.source_key,
                        "event_type": record.event_type,
                    }
                )

    return {
        "schema_version": 1,
        "audit_base_commit": AUDIT_BASE_COMMIT,
        "operation_count": len(rows),
        "operations": [
            {
                **asdict(observation),
                "records": [asdict(record) for record in observation.records],
            }
            for observation in rows
        ],
        "record_class_counts": {
            name: class_counts.get(name, 0) for name in sorted(PRIMARY_RECORD_CLASSES)
        },
        "canonical_transition_record_count": class_counts["CanonicalTransitionRecord"],
        "canonical_schema_matches": canonical_matches,
    }


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def build_evidence_manifest(
    directory: Path,
    *,
    branch: str,
    head: str,
    audit_base: str = AUDIT_BASE_COMMIT,
) -> dict[str, Any]:
    artifacts = []
    for path in sorted(directory.glob("*")):
        if not path.is_file() or path.name == "manifest.json":
            continue
        artifacts.append(
            {
                "path": path.name,
                "sha256": sha256_file(path),
                "size_bytes": path.stat().st_size,
            }
        )
    return {
        "schema_version": 1,
        "branch": branch,
        "head": head,
        "audit_base": audit_base,
        "artifacts": artifacts,
    }


def git_value(repo_root: Path, *args: str) -> str:
    return subprocess.check_output(
        ["git", *args], cwd=repo_root, text=True
    ).strip()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Build the Sprint 80 tamper-evident evidence manifest"
    )
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--directory", default="local_out/sprint80")
    parser.add_argument("--out", default="local_out/sprint80/manifest.json")
    args = parser.parse_args(argv)

    repo_root = Path(args.repo_root).resolve()
    directory = Path(args.directory)
    manifest = build_evidence_manifest(
        directory,
        branch=git_value(repo_root, "branch", "--show-current"),
        head=git_value(repo_root, "rev-parse", "HEAD"),
    )
    write_json(Path(args.out), manifest)
    print(json.dumps(manifest, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
