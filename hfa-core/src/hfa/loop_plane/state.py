from dataclasses import dataclass
from .model import LoopContract, Recommendation

@dataclass(frozen=True)
class LoopState:
    loop_id: str
    revision: int = 0
    started: bool = False
    closed: bool = False
    contract: LoopContract | None = None
    run_id: str | None = None
    task_id: str | None = None
    current_attempt_id: str | None = None
    attempt_count: int = 0
    terminal_recommendation: Recommendation | None = None
