"""
hfa_worker.runtime.terminal_duplicate_delivery
---------------------------------------------

Sprint 69.1 product runtime guard.

If the WorkerConsumer bridge receives a stream delivery that resolves to an
already-terminal task, it must classify the delivery before TaskConsumer is
entered.  This prevents duplicate execution, duplicate claim_start, and
duplicate completion.

This module is deliberately read-only:
- no ACK
- no XCLAIM
- no requeue
- no repair
- no claim
- no completion
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from hfa.dag.schema import DagRedisKey
from hfa_worker.task_context import TaskContext


TERMINAL_DUPLICATE_DELIVERY = "terminal_duplicate_delivery"
NOT_TERMINAL_DUPLICATE_DELIVERY = "not_terminal_duplicate_delivery"

ACK_POLICY_NOT_TERMINAL = "not_terminal"
ACK_POLICY_NO_ACK_WITHOUT_EXPLICIT_TASK_IDENTITY = "no_ack_without_explicit_task_identity"
ACK_POLICY_NO_ACK_WITHOUT_RUN_ID_MATCH = "no_ack_without_run_id_match"
ACK_POLICY_NO_ACK_WITHOUT_TERMINAL_EVIDENCE = "no_ack_without_terminal_evidence"
ACK_POLICY_ACK_EXPLICIT_TASK_RUN_TERMINAL_EVIDENCE = "ack_explicit_task_run_terminal_evidence"

_TERMINAL_STATES = frozenset(
    {
        "done",
        "failed",
        "cancelled",
        "dead_lettered",
        "rejected",
        "blocked_by_failure",
    }
)


@dataclass(frozen=True)
class TerminalDuplicateDeliveryDecision:
    task_id: str
    run_id: str
    state: str
    terminal: bool
    status: str
    suppress_claim: bool
    suppress_execution: bool
    suppress_completion: bool
    ack_allowed: bool
    reason: str
    ack_policy: str = ""
    message_task_id: str = ""
    message_run_id: str = ""
    evidence_run_id: str = ""
    message_identity_verified: bool = False
    terminal_evidence_verified: bool = False


def _decode(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def _mapping_get(mapping: Any, key: str) -> object:
    if not isinstance(mapping, dict):
        return ""
    return mapping.get(key) or mapping.get(key.encode("utf-8")) or ""


async def _read_task_meta(redis: object, task_id: str) -> dict[Any, Any]:
    hgetall = getattr(redis, "hgetall", None)
    if not callable(hgetall):
        return {}
    raw = await hgetall(DagRedisKey.task_meta(task_id))
    if isinstance(raw, dict):
        return raw
    return {}


def _resolve_ack_policy(
    *,
    ctx: TaskContext,
    state: str,
    terminal: bool,
    message_task_id: str,
    message_run_id: str,
    evidence_run_id: str,
) -> tuple[bool, str, bool, bool]:
    if not terminal:
        return False, ACK_POLICY_NOT_TERMINAL, False, False

    explicit_task_identity = bool(message_task_id) and message_task_id == ctx.task_id
    run_identity = bool(message_run_id) and message_run_id == ctx.run_id
    terminal_evidence = bool(evidence_run_id) and evidence_run_id == ctx.run_id and bool(state)

    if not explicit_task_identity:
        return (
            False,
            ACK_POLICY_NO_ACK_WITHOUT_EXPLICIT_TASK_IDENTITY,
            False,
            terminal_evidence,
        )

    if not run_identity:
        return (
            False,
            ACK_POLICY_NO_ACK_WITHOUT_RUN_ID_MATCH,
            False,
            terminal_evidence,
        )

    if not terminal_evidence:
        return (
            False,
            ACK_POLICY_NO_ACK_WITHOUT_TERMINAL_EVIDENCE,
            True,
            False,
        )

    return (
        True,
        ACK_POLICY_ACK_EXPLICIT_TASK_RUN_TERMINAL_EVIDENCE,
        True,
        True,
    )


async def classify_terminal_duplicate_delivery(
    redis: object,
    ctx: TaskContext,
    *,
    message_task_id: str = "",
    message_run_id: str = "",
) -> TerminalDuplicateDeliveryDecision:
    """
    Classify an already-terminal task delivery before TaskConsumer.consume_once().

    Sprint 69.2 ACK policy is intentionally strict:
    ACK is allowed only when explicit message task identity, message run identity,
    and terminal task evidence all match. Fallback task_id=run_id identity remains
    no-ACK.
    """
    state = _decode(await redis.get(DagRedisKey.task_state(ctx.task_id)))
    terminal = state in _TERMINAL_STATES
    meta = await _read_task_meta(redis, ctx.task_id) if terminal else {}
    evidence_run_id = _decode(_mapping_get(meta, "run_id"))

    ack_allowed, ack_policy, message_identity_verified, terminal_evidence_verified = (
        _resolve_ack_policy(
            ctx=ctx,
            state=state,
            terminal=terminal,
            message_task_id=message_task_id,
            message_run_id=message_run_id,
            evidence_run_id=evidence_run_id,
        )
    )

    if terminal:
        return TerminalDuplicateDeliveryDecision(
            task_id=ctx.task_id,
            run_id=ctx.run_id,
            state=state,
            terminal=True,
            status=TERMINAL_DUPLICATE_DELIVERY,
            suppress_claim=True,
            suppress_execution=True,
            suppress_completion=True,
            ack_allowed=ack_allowed,
            reason="task_already_terminal_before_claim",
            ack_policy=ack_policy,
            message_task_id=message_task_id,
            message_run_id=message_run_id,
            evidence_run_id=evidence_run_id,
            message_identity_verified=message_identity_verified,
            terminal_evidence_verified=terminal_evidence_verified,
        )

    return TerminalDuplicateDeliveryDecision(
        task_id=ctx.task_id,
        run_id=ctx.run_id,
        state=state,
        terminal=False,
        status=NOT_TERMINAL_DUPLICATE_DELIVERY,
        suppress_claim=False,
        suppress_execution=False,
        suppress_completion=False,
        ack_allowed=False,
        reason="task_not_terminal_before_claim",
        ack_policy=ack_policy,
        message_task_id=message_task_id,
        message_run_id=message_run_id,
        evidence_run_id=evidence_run_id,
        message_identity_verified=False,
        terminal_evidence_verified=False,
    )
