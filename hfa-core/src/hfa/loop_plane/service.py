from __future__ import annotations

from .decision import recommend
from .errors import DomainTransitionError, ModeViolation
from .model import (
    CanonicalCommandProposal,
    CommandReceipt,
    CriterionResult,
    DecisionResult,
    LoopContract,
    LoopEvent,
    LoopMode,
    ProposalResult,
    Recommendation,
    TranslationResult,
    canonical_hash,
    thaw,
)
from .translation import RuntimeEventTranslator
from .verification import verify


class LoopPlaneService:
    def __init__(self, store, mode: LoopMode = LoopMode.OFF):
        self.store = store
        self.mode = mode
        self.translator = RuntimeEventTranslator()

    def observe(self, data: dict, expected_revision: int, *, contract: LoopContract | None = None) -> TranslationResult:
        if self.mode is LoopMode.OFF:
            raise ModeViolation("OFF forbids observation and all writes")
        translated = self.translator.translate(data, expected_revision + 1, contract=contract)
        if translated.event is None:
            return translated
        event = translated.event
        receipt = CommandReceipt(
            operation="RUNTIME_EVENT_OBSERVE",
            loop_id=event.loop_id,
            idempotency_key=data["event_id"],
            command_hash=canonical_hash({"data": data, "contract_hash": contract.contract_hash if contract else None}),
            result_event_ids=(event.event_id,),
            resulting_revision=event.revision,
            causation_id=event.causation_id,
            correlation_id=event.correlation_id,
        )
        self.store.commit(event.loop_id, [event], receipt, expected_revision)
        return translated

    def evaluate(
        self,
        *,
        loop_id: str,
        attempt_id: str,
        criterion_results: list[CriterionResult],
        generator_principal: str,
        now_ms: int,
        expected_revision: int,
        idempotency_key: str,
        causation_id: str,
        correlation_id: str,
        defect_type: str | None = None,
    ) -> DecisionResult:
        if self.mode not in {LoopMode.SHADOW_DECIDE, LoopMode.PROPOSE}:
            raise ModeViolation("evaluation forbidden in current mode")
        state = self.store.load_state(loop_id)
        contract = self.store.load_contract(loop_id)
        if not state.started or contract is None:
            raise DomainTransitionError("started loop and contract required")
        if state.closed:
            raise DomainTransitionError("closed loop")
        if state.current_attempt_id != attempt_id:
            raise DomainTransitionError("current attempt mismatch")
        if state.current_input_hash is None or state.task_id is None or state.run_id is None:
            raise DomainTransitionError("attempt identity incomplete")

        outcomes, reasons, evidence_set_hash = verify(
            contract,
            criterion_results,
            loop_id=loop_id,
            attempt_id=attempt_id,
            task_id=state.task_id,
            run_id=state.run_id,
            input_hash=state.current_input_hash,
            now_ms=now_ms,
            generator_principal=generator_principal,
        )
        recommendation, reason_code = recommend(contract, outcomes, defect_type)
        if reasons:
            reason_code = f"{reason_code}:{','.join(reasons)}"

        command_payload = {
            "loop_id": loop_id,
            "attempt_id": attempt_id,
            "criterion_results": [thaw(row) for row in criterion_results],
            "generator_principal": generator_principal,
            "now_ms": now_ms,
            "defect_type": defect_type,
            "expected_revision": expected_revision,
        }
        command_hash = canonical_hash(command_payload)
        criteria_event_id = f"criteria:{command_hash}"
        decision_event_id = f"decision:{canonical_hash({'command_hash': command_hash, 'recommendation': recommendation.value})}"
        criteria_event = LoopEvent(
            event_id=criteria_event_id,
            loop_id=loop_id,
            revision=expected_revision + 1,
            event_type="CriteriaEvaluated",
            occurred_at_ms=now_ms,
            causation_id=causation_id,
            correlation_id=correlation_id,
            payload={
                "attempt_id": attempt_id,
                "outcomes": {key: value.value for key, value in sorted(outcomes.items())},
                "evidence_set_hash": evidence_set_hash,
                "contract_hash": contract.contract_hash,
                "policy_version": contract.policy_version,
            },
        )
        decision_event = LoopEvent(
            event_id=decision_event_id,
            loop_id=loop_id,
            revision=expected_revision + 2,
            event_type="ShadowDecisionRecorded",
            occurred_at_ms=now_ms,
            causation_id=causation_id,
            correlation_id=correlation_id,
            payload={
                "attempt_id": attempt_id,
                "recommendation": recommendation.value,
                "reason_code": reason_code,
                "evidence_set_hash": evidence_set_hash,
                "contract_hash": contract.contract_hash,
                "policy_version": contract.policy_version,
            },
        )
        receipt = CommandReceipt(
            operation="SHADOW_EVALUATE",
            loop_id=loop_id,
            idempotency_key=idempotency_key,
            command_hash=command_hash,
            result_event_ids=(criteria_event_id, decision_event_id),
            resulting_revision=expected_revision + 2,
            causation_id=causation_id,
            correlation_id=correlation_id,
        )
        committed = self.store.commit(loop_id, [criteria_event, decision_event], receipt, expected_revision)
        return DecisionResult(recommendation, reason_code, evidence_set_hash, decision_event_id, committed.resulting_revision, committed)

    def proposal(
        self,
        *,
        loop_id: str,
        attempt_id: str,
        decision_event_id: str,
        recommendation: Recommendation,
        requested_canonical_operation: str,
        expected_runtime_revision: int,
        reason_code: str,
        evidence_set_hash: str,
        source_transition_ids: tuple[str, ...],
        now_ms: int,
        expected_revision: int,
        idempotency_key: str,
        causation_id: str,
        correlation_id: str,
    ) -> ProposalResult:
        if self.mode is not LoopMode.PROPOSE:
            raise ModeViolation("proposal forbidden in current mode")
        state = self.store.load_state(loop_id)
        contract = self.store.load_contract(loop_id)
        if contract is None or state.decision_event_id is None:
            raise DomainTransitionError("committed decision required")
        decision_event = self.store.find_event(loop_id, decision_event_id)
        if decision_event is None or decision_event.event_type != "ShadowDecisionRecorded":
            raise DomainTransitionError("source decision event missing")
        if decision_event.payload["attempt_id"] != attempt_id:
            raise DomainTransitionError("proposal attempt mismatch")
        if decision_event.payload["recommendation"] != recommendation.value:
            raise DomainTransitionError("proposal recommendation mismatch")
        if decision_event.payload["evidence_set_hash"] != evidence_set_hash:
            raise DomainTransitionError("proposal evidence mismatch")
        if decision_event.payload["policy_version"] != contract.policy_version:
            raise DomainTransitionError("proposal policy mismatch")

        proposal_id = f"proposal:{canonical_hash({'decision_event_id': decision_event_id, 'operation': requested_canonical_operation})}"
        proposal = CanonicalCommandProposal(
            proposal_id=proposal_id,
            loop_id=loop_id,
            attempt_id=attempt_id,
            decision_event_id=decision_event_id,
            recommendation=recommendation,
            requested_canonical_operation=requested_canonical_operation,
            expected_runtime_revision=expected_runtime_revision,
            reason_code=reason_code,
            evidence_set_hash=evidence_set_hash,
            source_transition_ids=source_transition_ids,
            policy_version=contract.policy_version,
            created_at_ms=now_ms,
        )
        command_hash = canonical_hash(proposal)
        event_id = f"proposal-event:{command_hash}"
        event = LoopEvent(
            event_id=event_id,
            loop_id=loop_id,
            revision=expected_revision + 1,
            event_type="ProposalRecorded",
            occurred_at_ms=now_ms,
            causation_id=causation_id,
            correlation_id=correlation_id,
            payload={
                "attempt_id": attempt_id,
                "decision_event_id": decision_event_id,
                "recommendation": recommendation.value,
                "proposal": proposal,
            },
        )
        receipt = CommandReceipt(
            operation="PASSIVE_PROPOSAL",
            loop_id=loop_id,
            idempotency_key=idempotency_key,
            command_hash=command_hash,
            result_event_ids=(event_id,),
            resulting_revision=expected_revision + 1,
            causation_id=causation_id,
            correlation_id=correlation_id,
        )
        committed = self.store.commit(loop_id, [event], receipt, expected_revision)
        return ProposalResult(proposal, event_id, committed.resulting_revision, committed)
