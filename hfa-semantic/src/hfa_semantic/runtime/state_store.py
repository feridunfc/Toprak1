from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, List, Optional

from pydantic import BaseModel


class RuntimeState(BaseModel):
    """Represents a single state record in the store."""
    rule_id: str
    partition: str
    state: dict[str, Any]
    event_count: int = 0
    last_update_ms: float = 0.0
    ttl_ms: int = 0

    # Legacy field aliases — kept for backward compat with any code that
    # accessed these names on the old RuntimeState model.
    @property
    def partition_key(self) -> str:
        return self.partition

    @property
    def state_data(self) -> dict[str, Any]:
        return self.state


class StateStore(ABC):
    """Abstract state store. All implementations must satisfy this contract."""

    @abstractmethod
    async def get(self, rule_id: str, partition_key: str) -> Optional[RuntimeState]:
        raise NotImplementedError

    @abstractmethod
    async def put(
        self,
        rule_id: str,
        partition_key: str,
        state: dict[str, Any],
        ttl_ms: int,
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    async def delete(self, rule_id: str, partition_key: str) -> None:
        """Delete a single partition's state."""
        raise NotImplementedError

    @abstractmethod
    async def evict(self, rule_id: str, max_partitions: int) -> int:
        """
        Evict oldest partitions for rule_id until at most max_partitions remain.

        Returns the number of partitions actually evicted.
        """
        raise NotImplementedError

    @abstractmethod
    async def list_partitions(self, rule_id: str) -> List[str]:
        """List all known partition keys for a rule."""
        raise NotImplementedError

    async def close(self) -> None:
        """Graceful shutdown hook. Override if cleanup is needed."""
        pass
