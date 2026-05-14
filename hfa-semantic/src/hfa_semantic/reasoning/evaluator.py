from __future__ import annotations

from typing import Optional

from hfa_semantic.api.models import RuleMatch
from hfa_semantic.reasoning.models import SemanticRule
from hfa_semantic.runtime.state_store import StateStore


class IncrementalEvaluator:
    def __init__(self, state_store: StateStore, active_rules: list[SemanticRule]):
        self._store = state_store
        self._rules = {rule.rule_id: rule for rule in active_rules}

    def _check_conditions(self, event: dict, rule: SemanticRule) -> bool:
        for cond in rule.conditions:
            val = event.get(cond.field)
            if val is None:
                return False
            if cond.operator == "gt" and not (val > cond.value):
                return False
            if cond.operator == "eq" and not (val == cond.value):
                return False
        return True

    async def evaluate_event(self, event: dict, partition_key: str, event_time_ms: int) -> Optional[RuleMatch]:
        for rule_id, rule in self._rules.items():
            if not self._check_conditions(event, rule):
                continue
            state = await self._store.get(rule_id, partition_key) or {"count": 0, "first_seen_ms": event_time_ms}
            if rule.window_ms and (event_time_ms - state["first_seen_ms"]) > rule.window_ms:
                state = {"count": 0, "first_seen_ms": event_time_ms}
            state["count"] += 1
            if state["count"] >= rule.trigger_count:
                await self._store.evict(rule_id, partition_key)
                return RuleMatch(
                    rule_id=rule_id,
                    partition_key=partition_key,
                    matched_at_ms=event_time_ms,
                    severity=rule.action_severity,
                    confidence=rule.action_confidence,
                    reason=f"rule_triggered:{rule_id}",
                )
            ttl_seconds = max(1, rule.window_ms // 1000) if rule.window_ms else 60
            await self._store.put(rule_id, partition_key, state, ttl_seconds)
        return None
