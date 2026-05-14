
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class TaskCapabilitySpec:
    required_capabilities: list[str] = field(default_factory=list)

    def normalized(self) -> list[str]:
        return sorted({cap.strip().lower() for cap in self.required_capabilities if cap and cap.strip()})


@dataclass(frozen=True)
class WorkerCapabilitySpec:
    worker_id: str
    capabilities: list[str] = field(default_factory=list)

    def normalized(self) -> set[str]:
        return {cap.strip().lower() for cap in self.capabilities if cap and cap.strip()}
