from __future__ import annotations

from enum import Enum
from hfa_semantic.api.models import RawEvent


class PartitionStrategy(Enum):
    """Strategy for partitioning events"""
    ENTITY_ID = "entity_id"
    EVENT_TYPE = "event_type"
    METRIC_NAME = "metric_name"


class Partitioner:
    """Handles event partitioning logic"""
    def __init__(self, strategy: PartitionStrategy = PartitionStrategy.ENTITY_ID):
        self.strategy = strategy

    def partition(self, event: RawEvent) -> str:
        """Generate partition key for an event"""
        return partition_key(event, self.strategy.value)


def partition_key(event: RawEvent, mode: str = "entity_id") -> str:
    if mode == "entity_id":
        return event.entity_id
    if mode == "event_type":
        return event.event_type
    if mode == "metric_name":
        return event.metric_name or "_"
    raise ValueError(f"unsupported_partition_mode:{mode}")
