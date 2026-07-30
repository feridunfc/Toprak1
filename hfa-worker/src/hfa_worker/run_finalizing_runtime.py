"""Opt-in worker adapters for Sprint 83.2 RUN_TERMINATE coordination."""
from __future__ import annotations

import logging
import time
from typing import Any

from hfa_worker.consumer import CONSUMER_GROUP, WorkerConsumer
from hfa_worker.redis_utils import ack_message
from hfa_worker.runtime.task_context_builder import build_task_context_from_run_requested
from hfa_worker.runtime.terminal_duplicate_delivery import (
    TERMINAL_DUPLICATE_DELIVERY,
    classify_terminal_duplicate_delivery,
)
from hfa_worker.task_consumer import TaskConsumer

logger = logging.getLogger(__name__)


class RunFinalizingTaskConsumer(TaskConsumer):
    """TaskConsumer extension that can recover only a missing RUN_TERMINATE."""

    async def finalize_terminal_duplicate(
        self,
        ctx,
        *,
        terminal_state: str,
        finalized_at_ms: int | None = None,
    ) -> Any | None:
        manager = self._completion_manager
        if manager is None:
            return None
        if not bool(getattr(manager, "run_termination_binding_enabled", False)):
            return None
        finalize = getattr(manager, "finalize_run_from_tasks", None)
        if not callable(finalize):
            return None
        return await finalize(
            run_id=ctx.run_id,
            tenant_id=ctx.tenant_id,
            trigger_task_id=ctx.task_id,
            finalized_at_ms=(
                int(finalized_at_ms)
                if finalized_at_ms is not None
                else int(time.time() * 1000)
            ),
            worker_instance_id=ctx.worker_instance_id,
            trigger_terminal_state=terminal_state,
        )


class RunFinalizingWorkerConsumer(WorkerConsumer):
    """WorkerConsumer opt-in bridge with terminal-delivery RUN recovery.

    The normal nonterminal path remains the existing WorkerConsumer path. Only
    an already-terminal TASK delivery is intercepted, and only RUN_TERMINATE may
    execute before ACK. Claim, TASK execution, and TASK completion stay
    suppressed.
    """

    async def _process_message_via_task_consumer(
        self,
        event: object,
        msg_id: object,
        stream: str,
        shard: int,
    ) -> None:
        ctx = build_task_context_from_run_requested(
            event,
            worker_id=self._worker_id,
            worker_group=self._worker_group,
            shard=shard,
        )
        message_task_id = str(getattr(event, "task_id", "") or "")
        message_run_id = str(getattr(event, "run_id", "") or "")
        duplicate = await classify_terminal_duplicate_delivery(
            self._redis,
            ctx,
            message_task_id=message_task_id,
            message_run_id=message_run_id,
        )
        if duplicate.status != TERMINAL_DUPLICATE_DELIVERY:
            return await super()._process_message_via_task_consumer(
                event,
                msg_id,
                stream,
                shard,
            )

        logger.info(
            "Sprint 83.2 terminal duplicate intercepted run=%s task=%s state=%s",
            ctx.run_id,
            ctx.task_id,
            duplicate.state,
        )
        if not duplicate.ack_allowed:
            return

        finalize = getattr(
            self._task_consumer,
            "finalize_terminal_duplicate",
            None,
        )
        if not callable(finalize):
            logger.warning(
                "RUN_TERMINATE adapter missing; terminal duplicate remains pending "
                "run=%s task=%s",
                ctx.run_id,
                ctx.task_id,
            )
            return

        run_termination = await finalize(
            ctx,
            terminal_state=duplicate.state,
        )
        if run_termination is None or not bool(
            getattr(run_termination, "ack_allowed", False)
        ):
            logger.warning(
                "Terminal duplicate RUN_TERMINATE blocked ACK run=%s task=%s status=%s",
                ctx.run_id,
                ctx.task_id,
                getattr(run_termination, "status", "unavailable"),
            )
            return

        await ack_message(self._redis, stream, CONSUMER_GROUP, msg_id)
