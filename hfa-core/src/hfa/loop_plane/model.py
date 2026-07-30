from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum
from hashlib import sha256
from types import MappingProxyType
from typing import Any, Mapping
import json

class LoopMode(str, Enum): OFF='OFF'; OBSERVE='OBSERVE'; SHADOW_DECIDE='SHADOW_DECIDE'; PROPOSE='PROPOSE'
class Recommendation(str, Enum): ACCEPT='ACCEPT'; RETRY_RECOMMENDED='RETRY_RECOMMENDED'; REWORK_RECOMMENDED='REWORK_RECOMMENDED'; REPLAN_RECOMMENDED='REPLAN_RECOMMENDED'; BLOCK='BLOCK'; ESCALATE='ESCALATE'; UNKNOWN='UNKNOWN'
class Outcome(str, Enum): PASS='PASS'; FAIL='FAIL'; UNKNOWN='UNKNOWN'
class TranslationStatus(str, Enum): TRANSLATED='TRANSLATED'; UNSUPPORTED_EVENT_TYPE='UNSUPPORTED_EVENT_TYPE'; MISSING_REQUIRED_FIELD='MISSING_REQUIRED_FIELD'; INVALID_FIELD_TYPE='INVALID_FIELD_TYPE'; INVALID_IDENTITY='INVALID_IDENTITY'; INVALID_REVISION='INVALID_REVISION'; INVALID_TIMESTAMP='INVALID_TIMESTAMP'

def canonical_hash(value: Any) -> str:
    return sha256(json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, default=str).encode()).hexdigest()

def freeze(value: Any) -> Any:
    if isinstance(value, Mapping): return MappingProxyType({str(k): freeze(v) for k,v in value.items()})
    if isinstance(value, list): return tuple(freeze(v) for v in value)
    if isinstance(value, tuple): return tuple(freeze(v) for v in value)
    if isinstance(value, set): return frozenset(freeze(v) for v in value)
    return value

@dataclass(frozen=True)
class LoopContract:
    contract_id: str; version: str; policy_version: str; required_criteria: tuple[str,...]; max_attempts: int; max_rework_depth: int
    def __post_init__(self):
        if not all((self.contract_id,self.version,self.policy_version)): raise ValueError('contract identity required')
        if len(set(self.required_criteria)) != len(self.required_criteria): raise ValueError('duplicate required criteria')
        if self.max_attempts < 1 or self.max_rework_depth < 0: raise ValueError('invalid limits')
    @property
    def contract_hash(self)->str: return canonical_hash(self.__dict__)

@dataclass(frozen=True)
class LoopEvent:
    event_id: str; loop_id: str; revision: int; event_type: str; occurred_at_ms: int; causation_id: str; correlation_id: str; payload: Mapping[str,Any]=field(default_factory=dict)
    def __post_init__(self):
        if not all((self.event_id,self.loop_id,self.event_type,self.causation_id,self.correlation_id)): raise ValueError('event identity required')
        if self.revision < 1 or self.occurred_at_ms < 0: raise ValueError('invalid revision/timestamp')
        object.__setattr__(self,'payload',freeze(dict(self.payload)))

@dataclass(frozen=True)
class CriterionResult:
    criterion_id: str; outcome: Outcome; loop_id: str; attempt_id: str; task_id: str; run_id: str; input_hash: str; artifact_hash: str|None; evaluator_principal: str; evaluator_version: str; independence_scope: str; evidence_refs: tuple[str,...]; evidence_hash: str; produced_at_ms: int|None; valid_until_ms: int|None; source_transition_id: str; source_task_revision: int; canonical_claim_operation_id: str; claim_epoch_or_fence: str; execution_generation: int

@dataclass(frozen=True)
class CanonicalCommandProposal:
    proposal_id: str; loop_id: str; attempt_id: str; decision_event_id: str; recommendation: Recommendation; requested_canonical_operation: str; expected_runtime_revision: int; reason_code: str; evidence_set_hash: str; source_transition_ids: tuple[str,...]; policy_version: str; created_at_ms: int; requires_human_approval: bool=True; executable: bool=False; auto_submit: bool=False; approval_status: str='REQUIRED'
    def __post_init__(self):
        if not self.requires_human_approval or self.executable or self.auto_submit or self.approval_status!='REQUIRED': raise ValueError('proposal safety invariant')

@dataclass(frozen=True)
class CommandReceipt:
    operation: str; loop_id: str; idempotency_key: str; command_hash: str; result_event_ids: tuple[str,...]; resulting_revision: int; causation_id: str; correlation_id: str

@dataclass(frozen=True)
class TranslationResult:
    status: TranslationStatus; event: LoopEvent|None=None; errors: tuple[str,...]=()
