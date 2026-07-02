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

from hfa.dag.schema import DagRedisKey
from hfa_worker.task_context import TaskContext


TERMINAL_DUPLICATE_DELIVERY = "terminal_duplicate_delivery"
NOT_TERMINAL_DUPLICATE_DELIVERY = "not_terminal_duplicate_delivery"

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


def _decode(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


async def classify_terminal_duplicate_delivery(
    redis: object,
    ctx: TaskContext,
) -> TerminalDuplicateDeliveryDecision:
    """
    Classify an already-terminal task delivery before TaskConsumer.consume_once().

    The decision is intentionally conservative for Sprint 69.1:
    terminal duplicate delivery suppresses claim/execution/completion, but ACK is
    not allowed here. ACK policy is a separate Sprint 69.2 contract.
    """
    state = _decode(await redis.get(DagRedisKey.task_state(ctx.task_id)))
    terminal = state in _TERMINAL_STATES

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
            ack_allowed=False,
            reason="task_already_terminal_before_claim",
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
    )
