"""
hfa_worker/task_heartbeat.py
----------------------------
Fenced heartbeat loop carrying explicit task and RUN identity.
"""
from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass, field

from hfa_control.task_recovery import TaskHeartbeatManager


@dataclass
class HeartbeatLoop:
    heartbeat_manager: TaskHeartbeatManager
    task_id: str
    tenant_id: str
    worker_instance_id: str
    interval_ms: int
    run_id: str = ""
    claim_epoch: str = ""
    _task: asyncio.Task | None = field(default=None, repr=False, compare=False)
    _stopped: asyncio.Event | None = field(default=None, repr=False, compare=False)
    _ownership_lost: asyncio.Event | None = field(
        default=None,
        repr=False,
        compare=False,
    )
    _ownership_loss_status: str = field(
        default="",
        repr=False,
        compare=False,
    )

    async def _run(self) -> None:
        assert self._stopped is not None
        try:
            while not self._stopped.is_set():
                result = await self.heartbeat_manager.record_heartbeat(
                    task_id=self.task_id,
                    run_id=self.run_id,
                    tenant_id=self.tenant_id,
                    worker_id=self.worker_instance_id,
                    claim_epoch=self.claim_epoch,
                )
                if not result.ok:
                    import logging

                    logging.getLogger(__name__).warning(
                        "HeartbeatLoop rejected: task=%s run=%s status=%s — stopping",
                        self.task_id,
                        self.run_id,
                        result.status,
                    )
                    self._ownership_loss_status = str(result.status or "")
                    if self._ownership_lost is not None:
                        self._ownership_lost.set()
                    return
                try:
                    await asyncio.wait_for(
                        self._stopped.wait(),
                        timeout=max(self.interval_ms, 1) / 1000.0,
                    )
                except asyncio.TimeoutError:
                    pass
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            import logging

            logging.getLogger(__name__).error(
                "HeartbeatLoop failed: task=%s run=%s error=%s — failing ownership",
                self.task_id,
                self.run_id,
                exc,
            )
            self._ownership_loss_status = f"heartbeat_error:{type(exc).__name__}"
            if self._ownership_lost is not None:
                self._ownership_lost.set()

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._stopped = asyncio.Event()
        self._ownership_lost = asyncio.Event()
        self._ownership_loss_status = ""
        self._task = asyncio.create_task(self._run(), name=f"heartbeat:{self.task_id}")

    async def wait_for_ownership_loss(self) -> str:
        if self._ownership_lost is None:
            raise RuntimeError("HeartbeatLoop is not started")
        await self._ownership_lost.wait()
        return self._ownership_loss_status

    async def stop(self) -> None:
        if self._stopped is not None:
            self._stopped.set()
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
