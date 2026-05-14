
from __future__ import annotations

from hfa_control.effect_ledger import EffectLedger
from hfa_control.event_store import EventStore
from hfa_control.replay_engine import ReplayEngine


def build_event_store(redis):
    return EventStore(redis)


def build_replay_engine():
    return ReplayEngine()


def build_effect_ledger(redis):
    return EffectLedger(redis)
