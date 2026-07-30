from dataclasses import dataclass
from .model import LoopContract, Recommendation


@dataclass(frozen=True)
class LoopState:
    loop_id: str
    revision: int = 0
    started: bool = False
    closed: bool = False
    closure_reason: str | None = None
    contract: LoopContract | None = None
    run_id: str | None = None
    task_id: str | None = None
    current_attempt_id: str | None = None
    current_input_hash: str | None = None
    attempt_count: int = 0
    rework_depth: int = 0
    latest_recommendation: Recommendation | None = None
    decision_event_id: str | None = None
    decision_count: int = 0
