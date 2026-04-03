"""
hfa_worker/task_heartbeat.py
-----------------------------
IRONCLAD Sprint 2 — Fenced heartbeat loop.

Sprint 2 change: HeartbeatLoop carries claim_epoch from the TaskContext and
passes it on every heartbeat call.  TaskHeartbeatManager rejects heartbeats
whose claim_epoch does not match the stored value, making zombie heartbeats
deterministically rejectable after a task has been requeued and re-claimed.
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
    # Sprint 2: fence token from claim — must match Redis meta on each call
    claim_epoch: str = ""
    _task: asyncio.Task | None = field(default=None, repr=False, compare=False)
    _stopped: asyncio.Event | None = field(default=None, repr=False, compare=False)

    async def _run(self) -> None:
        assert self._stopped is not None
        try:
            while not self._stopped.is_set():
                result = await self.heartbeat_manager.record_heartbeat(
                    task_id=self.task_id,
                    tenant_id=self.tenant_id,
                    worker_id=self.worker_instance_id,
                    claim_epoch=self.claim_epoch,
                )
                if not result.ok:
                    # Heartbeat rejected — stop the loop so the worker
                    # does not keep writing liveness for a claim it no longer owns.
                    import logging
                    logging.getLogger(__name__).warning(
                        "HeartbeatLoop rejected: task=%s status=%s — stopping",
                        self.task_id, result.status,
                    )
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

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._stopped = asyncio.Event()
        self._task = asyncio.create_task(self._run(), name=f"heartbeat:{self.task_id}")

    async def stop(self) -> None:
        if self._stopped is not None:
            self._stopped.set()
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
