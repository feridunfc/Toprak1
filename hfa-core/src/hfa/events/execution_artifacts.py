"""
Claim-check artifact helpers for authoritative execution output capture.

The event log remains the authority.  Small outputs may be embedded directly in
an event payload.  Large outputs are stored through a claim-check reference and
only a deterministic content hash/reference is recorded in the event.  This
keeps replay deterministic without forcing live LLM calls or large event bodies.

Rollback:
    Disable IRON_V3_LLM_SEALING to bypass callers that opt into Sprint 3
    completion capture.  This helper is otherwise additive.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

_FALSE_VALUES = {"", "0", "false", "False", "no", "NO", "off", "OFF"}
DEFAULT_INLINE_LIMIT_BYTES = 4096


class ArtifactStore(Protocol):
    async def write_artifact(self, *, content: bytes, content_hash: str) -> str: ...

    async def read_artifact(self, artifact_ref: str) -> bytes: ...


@dataclass(frozen=True)
class SealedArtifact:
    mode: str
    content_hash: str
    size_bytes: int
    encoding: str = "json"
    inline_value: Any | None = None
    artifact_ref: str | None = None

    def to_event_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "mode": self.mode,
            "content_hash": self.content_hash,
            "size_bytes": self.size_bytes,
            "encoding": self.encoding,
        }
        if self.mode == "inline":
            payload["inline_value"] = self.inline_value
        else:
            payload["artifact_ref"] = self.artifact_ref
        return payload


class FileArtifactStore:
    """Small local claim-check store used by tests and single-node deployments."""

    def __init__(self, root: str | os.PathLike[str] | None = None) -> None:
        configured = root or os.getenv("IRON_V3_ARTIFACT_DIR") or ".ironclad_artifacts"
        self.root = Path(configured)

    async def write_artifact(self, *, content: bytes, content_hash: str) -> str:
        self.root.mkdir(parents=True, exist_ok=True)
        path = self.root / f"{uuid.uuid4().hex}-{content_hash[:16]}.json"
        path.write_bytes(content)
        return f"file://{path.resolve().as_posix()}"

    async def read_artifact(self, artifact_ref: str) -> bytes:
        if not artifact_ref.startswith("file://"):
            raise ValueError(f"unsupported artifact reference: {artifact_ref}")
        path = Path(artifact_ref[len("file://"):])
        return path.read_bytes()


def is_llm_sealing_enabled() -> bool:
    return os.getenv("IRON_V3_LLM_SEALING", "0") not in _FALSE_VALUES


def _json_default(value: Any) -> Any:
    if isinstance(value, bytes):
        return {"__bytes_b64__": base64.b64encode(value).decode("ascii")}
    if isinstance(value, set):
        return sorted(value)
    if hasattr(value, "model_dump"):
        return value.model_dump()
    if hasattr(value, "__dict__"):
        return value.__dict__
    return repr(value)


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        default=_json_default,
        ensure_ascii=False,
    ).encode("utf-8")


def sha256_hex(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


async def seal_execution_artifact(
    value: Any,
    *,
    artifact_store: ArtifactStore | None = None,
    inline_limit_bytes: int = DEFAULT_INLINE_LIMIT_BYTES,
) -> SealedArtifact:
    """Return an event-safe representation of an execution output value."""
    content = canonical_json_bytes(value)
    content_hash = sha256_hex(content)
    size = len(content)
    if size <= inline_limit_bytes:
        return SealedArtifact(
            mode="inline",
            content_hash=content_hash,
            size_bytes=size,
            inline_value=value,
        )

    store = artifact_store or FileArtifactStore()
    artifact_ref = await store.write_artifact(content=content, content_hash=content_hash)
    return SealedArtifact(
        mode="claim_check",
        content_hash=content_hash,
        size_bytes=size,
        artifact_ref=artifact_ref,
    )


async def resolve_execution_artifact(
    sealed_payload: dict[str, Any],
    *,
    artifact_store: ArtifactStore | None = None,
) -> Any:
    """Resolve a sealed event payload without invoking a live LLM."""
    mode = sealed_payload.get("mode")
    if mode == "inline":
        return sealed_payload.get("inline_value")
    if mode != "claim_check":
        raise ValueError(f"unsupported sealed artifact mode: {mode}")

    artifact_ref = sealed_payload.get("artifact_ref")
    expected_hash = sealed_payload.get("content_hash")
    if not artifact_ref or not expected_hash:
        raise ValueError("claim-check artifact requires artifact_ref and content_hash")

    store = artifact_store or FileArtifactStore()
    content = await store.read_artifact(str(artifact_ref))
    actual_hash = sha256_hex(content)
    if actual_hash != expected_hash:
        raise ValueError("claim-check artifact checksum mismatch")
    return json.loads(content.decode("utf-8"))
