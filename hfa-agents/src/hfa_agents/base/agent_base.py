from __future__ import annotations

import asyncio
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from hfa_agents.base.contracts import EnrichedEvent, ExecutionResult
from hfa.events.completion_capture import CompletionCaptureError, maybe_capture_agent_completion


@dataclass(slots=True)
class CircuitBreaker:
    threshold: int = 3
    recovery_timeout_seconds: int = 60
    failures: int = 0
    is_open: bool = False
    last_failure_at: Optional[datetime] = None

    def check(self) -> bool:
        if not self.is_open:
            return True
        if self.last_failure_at is None:
            return False
        elapsed = (datetime.now(timezone.utc) - self.last_failure_at).total_seconds()
        if elapsed >= self.recovery_timeout_seconds:
            self.failures = 0
            self.is_open = False
            return True
        return False

    def record_success(self) -> None:
        self.failures = 0
        self.is_open = False
        self.last_failure_at = None

    def record_failure(self) -> None:
        self.failures += 1
        self.last_failure_at = datetime.now(timezone.utc)
        if self.failures >= self.threshold:
            self.is_open = True


class AgentBase(ABC):
    role: str = "base"

    def __init__(self, agent_id: str, semantic_engine, timeout_seconds: int = 120) -> None:
        self.agent_id = agent_id
        self.semantic_engine = semantic_engine
        self.timeout_seconds = timeout_seconds
        self.circuit_breaker = CircuitBreaker()
        self._lock = asyncio.Lock()

    async def execute(self, event: EnrichedEvent) -> ExecutionResult:
        async with self._lock:
            if not self.circuit_breaker.check():
                return ExecutionResult(
                    status="escalated",
                    reasoning_trace=[f"circuit_open:{self.agent_id}"],
                    requires_hitl=True,
                )

            started = time.perf_counter()
            try:
                result = await asyncio.wait_for(self._execute_core(event), timeout=self.timeout_seconds)
                receipt = await maybe_capture_agent_completion(
                    event=event,
                    result=result,
                    role=self.role,
                    agent_id=self.agent_id,
                )
                if receipt is not None:
                    result.artifacts["completion_capture"] = receipt.to_event_details()
                    result.reasoning_trace.append("completion_capture=sealed")
                self.circuit_breaker.record_success()
                result.reasoning_trace.append(f"agent={self.agent_id}")
                result.reasoning_trace.append(f"duration_ms={(time.perf_counter() - started)*1000:.2f}")
                return result
            except CompletionCaptureError as exc:
                self.circuit_breaker.record_failure()
                return ExecutionResult(
                    status="failed",
                    reasoning_trace=[f"completion_capture_failed:{self.agent_id}:{exc}"],
                    suggested_feedback="authoritative completion capture failed",
                    requires_hitl=True,
                )
            except asyncio.TimeoutError:
                self.circuit_breaker.record_failure()
                return ExecutionResult(
                    status="failed",
                    reasoning_trace=[f"timeout:{self.agent_id}:{self.timeout_seconds}s"],
                    suggested_feedback=f"{self.agent_id} timed out",
                    requires_hitl=True,
                )
            except Exception as exc:  # pragma: no cover - integration path
                self.circuit_breaker.record_failure()
                return ExecutionResult(
                    status="failed",
                    reasoning_trace=[f"exception:{self.agent_id}:{type(exc).__name__}:{exc}"],
                    suggested_feedback=f"{self.agent_id} crashed",
                    requires_hitl=True,
                )

    @abstractmethod
    async def _execute_core(self, event: EnrichedEvent) -> ExecutionResult:
        raise NotImplementedError
