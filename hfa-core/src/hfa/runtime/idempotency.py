
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class IdempotencyResult:
    accepted: bool
    reason: str
    token_key: str
    existing_value: str = ""


class IdempotencyKeys:
    @staticmethod
    def dispatch_token(task_id: str) -> str:
        return f"hfa:task:{task_id}:dispatch_token"

    @staticmethod
    def completion_token(task_id: str) -> str:
        return f"hfa:task:{task_id}:completion_token"
