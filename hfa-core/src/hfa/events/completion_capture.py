"""
Authoritative completion capture for LLM/agent outputs.

Sprint 3 seals authoritative LLM-like outputs before they become replay-relevant.
Replay consumes captured events and claim-check artifacts; it must never call a
live model to reconstruct output.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Optional, Protocol

from hfa.events.execution_artifacts import (
    ArtifactStore,
    canonical_json_bytes,
    is_llm_sealing_enabled,
    resolve_execution_artifact,
    seal_execution_artifact,
    sha256_hex,
)

logger = logging.getLogger(__name__)

LLM_COMPLETION_CAPTURED = "LLM_COMPLETION_CAPTURED"


class CompletionEventAppender(Protocol):
    async def append_event(
        self,
        *,
        run_id: str,
        event_type: str,
        worker_id: Optional[str] = None,
        details: Optional[dict[str, Any]] = None,
    ) -> bool: ...


class CompletionCaptureError(RuntimeError):
    """Raised when strict completion capture cannot be durably recorded."""


@dataclass(frozen=True)
class CompletionCaptureReceipt:
    run_id: str
    event_type: str
    provider: str
    model: str
    prompt_hash: str
    system_prompt_hash: str
    output_hash: str
    tokens: int
    cost_cents: int
    fallback_used: bool
    output: dict[str, Any]

    def to_event_details(self) -> dict[str, Any]:
        return {
            "provider": self.provider,
            "model": self.model,
            "prompt_hash": self.prompt_hash,
            "system_prompt_hash": self.system_prompt_hash,
            "output_hash": self.output_hash,
            "tokens": self.tokens,
            "cost_cents": self.cost_cents,
            "fallback_used": self.fallback_used,
            "output": self.output,
            "capture_helper": "hfa.events.completion_capture.capture_agent_completion",
        }


def _hash_value(value: Any) -> str:
    return sha256_hex(canonical_json_bytes(value))


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _safe_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).lower() in {"1", "true", "yes", "on"}


def _metadata_from(event: Any, result: Any, *, role: str, agent_id: str) -> dict[str, Any]:
    context = getattr(event, "context", {}) or {}
    artifacts = getattr(result, "artifacts", {}) or {}
    output_data = getattr(result, "output_data", {}) or {}
    return {
        "provider": artifacts.get("provider") or context.get("provider") or "agent",
        "model": artifacts.get("model") or context.get("model") or role,
        "prompt_hash": artifacts.get("prompt_hash") or _hash_value(context.get("prompt", getattr(event, "goal", ""))),
        "system_prompt_hash": artifacts.get("system_prompt_hash") or _hash_value(context.get("system_prompt", "")),
        "output_hash": artifacts.get("output_hash") or _hash_value(output_data),
        "tokens": _safe_int(artifacts.get("tokens", context.get("tokens", 0))),
        "cost_cents": _safe_int(artifacts.get("cost_cents", context.get("cost_cents", 0))),
        "fallback_used": _safe_bool(artifacts.get("fallback_used", context.get("fallback_used", False))),
        "run_id": context.get("run_id") or getattr(event, "lineage_run_id", None) or getattr(event, "event_id", agent_id),
    }


async def capture_agent_completion(
    *,
    event: Any,
    result: Any,
    role: str,
    agent_id: str,
    event_appender: CompletionEventAppender | None,
    artifact_store: ArtifactStore | None = None,
    inline_limit_bytes: int | None = None,
) -> CompletionCaptureReceipt:
    """Seal a role result and append an authoritative capture event."""
    meta = _metadata_from(event, result, role=role, agent_id=agent_id)
    kwargs: dict[str, Any] = {"artifact_store": artifact_store}
    if inline_limit_bytes is not None:
        kwargs["inline_limit_bytes"] = inline_limit_bytes
    sealed = await seal_execution_artifact(getattr(result, "output_data", {}) or {}, **kwargs)

    receipt = CompletionCaptureReceipt(
        run_id=str(meta["run_id"]),
        event_type=LLM_COMPLETION_CAPTURED,
        provider=str(meta["provider"]),
        model=str(meta["model"]),
        prompt_hash=str(meta["prompt_hash"]),
        system_prompt_hash=str(meta["system_prompt_hash"]),
        output_hash=str(meta["output_hash"]),
        tokens=int(meta["tokens"]),
        cost_cents=int(meta["cost_cents"]),
        fallback_used=bool(meta["fallback_used"]),
        output=sealed.to_event_payload(),
    )

    if event_appender is None:
        raise CompletionCaptureError("IRON_V3_LLM_SEALING requires an event appender")

    appended = await event_appender.append_event(
        run_id=receipt.run_id,
        event_type=LLM_COMPLETION_CAPTURED,
        worker_id=agent_id,
        details=receipt.to_event_details(),
    )
    if not appended:
        raise CompletionCaptureError("completion capture append returned false")
    return receipt


async def maybe_capture_agent_completion(
    *,
    event: Any,
    result: Any,
    role: str,
    agent_id: str,
) -> CompletionCaptureReceipt | None:
    """Feature-flagged capture wrapper used by AgentBase."""
    if not is_llm_sealing_enabled():
        return None
    context = getattr(event, "context", {}) or {}
    event_appender = context.get("event_appender") or context.get("event_store")
    artifact_store = context.get("artifact_store")
    inline_limit = context.get("completion_capture_inline_limit_bytes")
    try:
        inline_limit_int = int(inline_limit) if inline_limit is not None else None
    except Exception:
        inline_limit_int = None
    return await capture_agent_completion(
        event=event,
        result=result,
        role=role,
        agent_id=agent_id,
        event_appender=event_appender,
        artifact_store=artifact_store,
        inline_limit_bytes=inline_limit_int,
    )


async def replay_captured_completion(
    event_details: dict[str, Any],
    *,
    artifact_store: ArtifactStore | None = None,
) -> Any:
    """Reconstruct captured output from event payload only; never call an LLM."""
    if event_details.get("event_type") == LLM_COMPLETION_CAPTURED and "details" in event_details:
        event_details = event_details["details"]
    output_payload = event_details.get("output")
    if not isinstance(output_payload, dict):
        raise ValueError("captured completion event missing output payload")
    return await resolve_execution_artifact(output_payload, artifact_store=artifact_store)
