from __future__ import annotations
from enum import Enum


class LatenessAction(str, Enum):
    DROP = "drop"
    ACCEPT_WITH_CORRECTION = "correct"
    SIDE_CHANNEL = "side_channel"


class LatenessPolicy:
    def __init__(self, action: LatenessAction = LatenessAction.DROP):
        self.action = action

    # ── Class-level shortcuts (tests use LatenessPolicy.DROP) ─────────────────
    DROP = LatenessAction.DROP
    ACCEPT_WITH_CORRECTION = LatenessAction.ACCEPT_WITH_CORRECTION
    SIDE_CHANNEL = LatenessAction.SIDE_CHANNEL
