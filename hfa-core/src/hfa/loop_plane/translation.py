from __future__ import annotations

from .model import LoopContract, LoopEvent, TranslationResult, TranslationStatus, canonical_hash

RUNTIME_EVENT_COMPATIBILITY = {
    "TaskAdmitted": {
        "source_file": "hfa-core/src/hfa/lua/task_admit.lua (canonical admission path; event fields vary by producer)",
        "loop_event_type": "LoopStarted",
        "requires_contract": True,
    },
    "TaskClaimed": {
        "source_file": "worker/task claim lifecycle path",
        "loop_event_type": "AttemptObserved",
        "requires_contract": False,
    },
    "TaskCompleted": {
        "source_file": "TaskConsumer completion lifecycle path",
        "loop_event_type": "RuntimeObservationRecorded",
        "requires_contract": False,
    },
    "TaskFailed": {
        "source_file": "task failure lifecycle path",
        "loop_event_type": "RuntimeObservationRecorded",
        "requires_contract": False,
    },
    "RunCompleted": {
        "source_file": "hfa-core/src/hfa/lua/run_terminate_from_tasks.lua results stream",
        "loop_event_type": "RuntimeObservationRecorded",
        "requires_contract": False,
    },
    "RunFailed": {
        "source_file": "hfa-core/src/hfa/lua/run_terminate_from_tasks.lua results stream",
        "loop_event_type": "RuntimeObservationRecorded",
        "requires_contract": False,
    },
}

_COMMON_REQUIRED = (
    "event_id", "event_type", "run_id", "task_id", "canonical_operation_id",
    "source_transition_id", "source_task_revision", "occurred_at_ms",
    "correlation_id", "causation_id",
)


def _nonempty_string(value) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _integer(value, minimum: int = 0) -> bool:
    return not isinstance(value, bool) and isinstance(value, int) and value >= minimum


class RuntimeEventTranslator:
    def translate(self, data: dict, revision: int, *, contract: LoopContract | None = None) -> TranslationResult:
        if not isinstance(data, dict):
            return TranslationResult(TranslationStatus.INVALID_FIELD_TYPE, errors=("event",))
        if not _integer(revision, 1):
            return TranslationResult(TranslationStatus.INVALID_REVISION, errors=("revision",))

        event_type = data.get("event_type")
        spec = RUNTIME_EVENT_COMPATIBILITY.get(event_type)
        if spec is None:
            return TranslationResult(TranslationStatus.UNSUPPORTED_EVENT_TYPE, errors=("event_type",))

        missing = tuple(key for key in _COMMON_REQUIRED if data.get(key) in (None, ""))
        if missing:
            return TranslationResult(TranslationStatus.MISSING_REQUIRED_FIELD, errors=missing)

        identity_fields = (
            "event_id", "event_type", "run_id", "task_id", "canonical_operation_id",
            "source_transition_id", "correlation_id", "causation_id",
        )
        invalid_identity = tuple(key for key in identity_fields if not _nonempty_string(data.get(key)))
        if invalid_identity:
            return TranslationResult(TranslationStatus.INVALID_IDENTITY, errors=invalid_identity)
        if not _integer(data.get("source_task_revision"), 0):
            return TranslationResult(TranslationStatus.INVALID_REVISION, errors=("source_task_revision",))
        if not _integer(data.get("occurred_at_ms"), 0):
            return TranslationResult(TranslationStatus.INVALID_TIMESTAMP, errors=("occurred_at_ms",))

        derived_loop_id = f"loop:{data['run_id']}"
        explicit_loop_id = data.get("loop_id")
        if explicit_loop_id is not None and explicit_loop_id != derived_loop_id:
            return TranslationResult(TranslationStatus.IDENTITY_CONFLICT, errors=("loop_id",))
        loop_id = derived_loop_id

        payload = {
            "source_event_id": data["event_id"],
            "source_event_type": event_type,
            "source_event_hash": canonical_hash(data),
            "run_id": data["run_id"],
            "task_id": data["task_id"],
            "canonical_operation_id": data["canonical_operation_id"],
            "source_transition_id": data["source_transition_id"],
            "source_task_revision": data["source_task_revision"],
        }

        if event_type == "TaskAdmitted":
            if contract is None:
                return TranslationResult(
                    TranslationStatus.PROVENANCE_INSUFFICIENT,
                    errors=("contract_id", "contract_version", "contract_hash", "policy_version"),
                )
            supplied_contract_hash = data.get("contract_hash")
            if supplied_contract_hash is not None and supplied_contract_hash != contract.contract_hash:
                return TranslationResult(TranslationStatus.IDENTITY_CONFLICT, errors=("contract_hash",))
            payload.update({
                "contract": contract,
                "contract_id": contract.contract_id,
                "contract_version": contract.version,
                "contract_hash": contract.contract_hash,
                "policy_version": contract.policy_version,
            })
        elif event_type == "TaskClaimed":
            generation = data.get("execution_generation")
            fence = data.get("claim_fence")
            input_hash = data.get("input_hash")
            if not _integer(generation, 0):
                return TranslationResult(TranslationStatus.INVALID_REVISION, errors=("execution_generation",))
            if not _nonempty_string(fence) or not _nonempty_string(input_hash):
                missing_provenance = tuple(
                    key for key, value in (("claim_fence", fence), ("input_hash", input_hash))
                    if not _nonempty_string(value)
                )
                return TranslationResult(TranslationStatus.PROVENANCE_INSUFFICIENT, errors=missing_provenance)
            derived_attempt_id = f"attempt:{data['task_id']}:{generation}:{fence}"
            explicit_attempt_id = data.get("attempt_id")
            if explicit_attempt_id is not None and explicit_attempt_id != derived_attempt_id:
                return TranslationResult(TranslationStatus.IDENTITY_CONFLICT, errors=("attempt_id",))
            payload.update({
                "attempt_id": derived_attempt_id,
                "execution_generation": generation,
                "claim_fence": fence,
                "input_hash": input_hash,
            })
        else:
            for optional in (
                "attempt_id", "artifact_or_output_ref", "source_run_revision",
                "claim_fence", "execution_generation", "input_hash",
            ):
                if optional in data:
                    payload[optional] = data[optional]

        event = LoopEvent(
            event_id=f"loop-event:{canonical_hash({'source_event_id': data['event_id'], 'loop_id': loop_id})}",
            loop_id=loop_id,
            revision=revision,
            event_type=spec["loop_event_type"],
            occurred_at_ms=data["occurred_at_ms"],
            causation_id=data["causation_id"],
            correlation_id=data["correlation_id"],
            payload=payload,
        )
        return TranslationResult(TranslationStatus.TRANSLATED, event)
