"""Opt-in worker adapters for Sprint 83.2 RUN_TERMINATE coordination."""
from __future__ import annotations

import logging
import time
from typing import Any

from hfa.dag.schema import DagRedisKey
from hfa_control.task_terminal_authority import (
    TASK_TERMINAL_NOT_COMMITTED_STATUS,
    TaskTerminalAuthorityError,
)
from hfa_worker.consumer import CONSUMER_GROUP, WorkerConsumer
from hfa_worker.redis_utils import ack_message
from hfa_worker.runtime.task_context_builder import build_task_context_from_run_requested
from hfa_worker.runtime.terminal_duplicate_delivery import (
    TERMINAL_DUPLICATE_DELIVERY,
    classify_terminal_duplicate_delivery,
)
from hfa_worker.task_consumer import TaskConsumer

logger = logging.getLogger(__name__)


def _text(value: object) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return "" if value is None else str(value)


class RunFinalizingTaskConsumer(TaskConsumer):
    """TaskConsumer extension that can recover only a missing RUN_TERMINATE."""

    async def finalize_terminal_duplicate(
        self,
        ctx,
        *,
        terminal_state: str,
        finalized_at_ms: int | None = None,
        authority_worker_instance_id: str | None = None,
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
            worker_instance_id=(
                authority_worker_instance_id or ctx.worker_instance_id
            ),
            trigger_terminal_state=terminal_state,
        )


class RunFinalizingWorkerConsumer(WorkerConsumer):
    """Worker bridge for RUN finalization and proof-bound redelivery recovery.

    Runtime-terminal deliveries retain the 84.7D RUN-only recovery path. In
    canonical Profile D only, a runtime ``running`` task may additionally prove
    that canonical TASK terminal authority is already durable, replay only its
    missing projection, then resume RUN_TERMINATE. No claim, executor call, new
    TASK terminal operation, or TASK_REQUEUE is reachable on that recovery path.
    """

    def __init__(
        self,
        *args: Any,
        task_terminal_authority_binding: Any | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._task_terminal_authority_binding = task_terminal_authority_binding

    async def _load_terminal_recovery_claim_input(
        self,
        ctx: Any,
    ) -> dict[str, str] | None:
        """Read mutable claim metadata only as lookup input for durable proof.

        The values below are never treated as authority. The frozen
        TaskTerminalAuthorityBinding validates them against the durable TASK
        terminal record/receipt, canonical head, and predecessor TASK_CLAIM.
        """
        state = _text(
            await self._redis.get(DagRedisKey.task_state(ctx.task_id))
        )
        if state != "running":
            return None

        raw = await self._redis.hgetall(DagRedisKey.task_meta(ctx.task_id))
        meta = {
            _text(key): _text(value)
            for key, value in (raw or {}).items()
        }
        expected_identity = {
            "task_id": ctx.task_id,
            "run_id": ctx.run_id,
            "tenant_id": ctx.tenant_id,
        }
        for field, expected in expected_identity.items():
            if meta.get(field, "") != expected:
                raise RuntimeError(
                    f"terminal recovery task_meta.{field} mismatch"
                )

        worker_instance_id = meta.get("worker_instance_id", "").strip()
        scheduler_epoch = meta.get("scheduler_epoch", "").strip()
        claim_epoch = meta.get("claim_epoch", "").strip()
        if (
            not worker_instance_id
            or not scheduler_epoch
            or scheduler_epoch == "0"
            or not claim_epoch.isdecimal()
            or int(claim_epoch) < 1
        ):
            raise RuntimeError(
                "terminal recovery claim-generation metadata is incomplete"
            )
        return {
            **expected_identity,
            "worker_instance_id": worker_instance_id,
            "scheduler_epoch": scheduler_epoch,
            "claim_epoch": claim_epoch,
        }

    async def _finalize_run_and_ack(
        self,
        ctx: Any,
        *,
        terminal_state: str,
        msg_id: object,
        stream: str,
        authority_worker_instance_id: str | None = None,
    ) -> None:
        finalize = getattr(
            self._task_consumer,
            "finalize_terminal_duplicate",
            None,
        )
        if not callable(finalize):
            logger.warning(
                "RUN_TERMINATE adapter missing; delivery remains pending "
                "run=%s task=%s",
                ctx.run_id,
                ctx.task_id,
            )
            return

        run_termination = await finalize(
            ctx,
            terminal_state=terminal_state,
            authority_worker_instance_id=authority_worker_instance_id,
        )
        if run_termination is None or not bool(
            getattr(run_termination, "ack_allowed", False)
        ):
            logger.warning(
                "RUN_TERMINATE blocked ACK run=%s task=%s status=%s",
                ctx.run_id,
                ctx.task_id,
                getattr(run_termination, "status", "unavailable"),
            )
            return

        await ack_message(self._redis, stream, CONSUMER_GROUP, msg_id)

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
        if duplicate.status == TERMINAL_DUPLICATE_DELIVERY:
            logger.info(
                "Terminal duplicate intercepted run=%s task=%s state=%s",
                ctx.run_id,
                ctx.task_id,
                duplicate.state,
            )
            if not duplicate.ack_allowed:
                return
            await self._finalize_run_and_ack(
                ctx,
                terminal_state=duplicate.state,
                msg_id=msg_id,
                stream=stream,
            )
            return

        binding = self._task_terminal_authority_binding
        if binding is None:
            return await super()._process_message_via_task_consumer(
                event,
                msg_id,
                stream,
                shard,
            )

        try:
            lookup = await self._load_terminal_recovery_claim_input(ctx)
        except Exception as exc:
            logger.error(
                "Canonical terminal recovery metadata failed closed "
                "run=%s task=%s error=%s",
                ctx.run_id,
                ctx.task_id,
                exc,
            )
            return

        if lookup is None:
            return await super()._process_message_via_task_consumer(
                event,
                msg_id,
                stream,
                shard,
            )

        try:
            terminal = await binding.replay_terminal_projection(
                task_id=lookup["task_id"],
                run_id=lookup["run_id"],
                tenant_id=lookup["tenant_id"],
                worker_instance_id=lookup["worker_instance_id"],
                scheduler_epoch=lookup["scheduler_epoch"],
                claim_epoch=lookup["claim_epoch"],
            )
        except TaskTerminalAuthorityError as exc:
            if exc.status == TASK_TERMINAL_NOT_COMMITTED_STATUS:
                return await super()._process_message_via_task_consumer(
                    event,
                    msg_id,
                    stream,
                    shard,
                )
            logger.error(
                "Canonical terminal projection recovery failed closed "
                "run=%s task=%s status=%s error=%s",
                ctx.run_id,
                ctx.task_id,
                exc.status,
                exc,
            )
            return
        except Exception as exc:
            logger.error(
                "Canonical terminal projection recovery unavailable "
                "run=%s task=%s error=%s",
                ctx.run_id,
                ctx.task_id,
                exc,
            )
            return

        if not bool(getattr(terminal, "completed", False)):
            logger.warning(
                "Canonical terminal replay did not complete run=%s task=%s status=%s",
                ctx.run_id,
                ctx.task_id,
                getattr(terminal, "status", "unavailable"),
            )
            return

        logger.info(
            "Recovered durable TASK terminal projection without execution "
            "run=%s task=%s state=%s",
            ctx.run_id,
            ctx.task_id,
            terminal.terminal_state,
        )
        await self._finalize_run_and_ack(
            ctx,
            terminal_state=terminal.terminal_state,
            msg_id=msg_id,
            stream=stream,
            authority_worker_instance_id=lookup["worker_instance_id"],
        )
