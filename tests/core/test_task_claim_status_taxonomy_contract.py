import inspect
import re
from pathlib import Path

import hfa_control.dag_lua as dag_lua


EXPECTED_TASK_CLAIM_STATUSES = frozenset(
    {
        dag_lua.TASK_CLAIM_STATUS_TASK_CLAIMED,
        dag_lua.TASK_CLAIM_STATUS_TASK_MISSING,
        dag_lua.TASK_CLAIM_STATUS_TASK_ALREADY_OWNED,
        dag_lua.TASK_CLAIM_STATUS_TASK_STATE_CONFLICT,
        dag_lua.TASK_CLAIM_STATUS_RESERVATION_MISSING,
        dag_lua.TASK_CLAIM_STATUS_RESERVATION_WORKER_MISMATCH,
        dag_lua.TASK_CLAIM_STATUS_RESERVATION_TASK_MISMATCH,
        dag_lua.TASK_CLAIM_STATUS_RESERVATION_EPOCH_MISMATCH,
        dag_lua.TASK_CLAIM_STATUS_MISSING_TASK_META,
        dag_lua.TASK_CLAIM_STATUS_IDENTITY_TASK_ID_MISSING,
        dag_lua.TASK_CLAIM_STATUS_IDENTITY_TASK_ID_MISMATCH,
        dag_lua.TASK_CLAIM_STATUS_IDENTITY_RUN_ID_MISSING,
        dag_lua.TASK_CLAIM_STATUS_IDENTITY_RUN_ID_MISMATCH,
        dag_lua.TASK_CLAIM_STATUS_RUN_TRUTH_MISSING,
        dag_lua.TASK_CLAIM_STATUS_RUN_TRUTH_TERMINAL_CONFLICT,
        dag_lua.TASK_CLAIM_STATUS_RUN_TRUTH_CORRUPTION_CONFLICT,
        (
            dag_lua
            .TASK_CLAIM_STATUS_TRUTH_CONFLICT_EVIDENCE_STORE_UNAVAILABLE
        ),
    }
)


def _lua_task_claim_return_statuses() -> set[str]:
    source = Path(
        "hfa-core/src/hfa/lua/task_claim_start.lua"
    ).read_text(encoding="utf-8")

    patterns = (
        r"claim_failure\(\s*['\"]([^'\"]+)['\"]",
        r"emit_truth_conflict\(\s*['\"]([^'\"]+)['\"]",
        r"return\s*\{\s*['\"]([^'\"]+)['\"]",
    )

    statuses: set[str] = set()
    for pattern in patterns:
        statuses.update(
            re.findall(
                pattern,
                source,
                flags=re.MULTILINE,
            )
        )
    return statuses


def test_python_task_claim_taxonomy_matches_expected_contract():
    assert (
        dag_lua.TASK_CLAIM_STATUSES
        == EXPECTED_TASK_CLAIM_STATUSES
    )
    assert dag_lua.TASK_CLAIM_SUCCESS_STATUSES == frozenset(
        {dag_lua.TASK_CLAIM_STATUS_TASK_CLAIMED}
    )
    assert dag_lua.TASK_CLAIM_FAILURE_STATUSES == (
        EXPECTED_TASK_CLAIM_STATUSES
        - dag_lua.TASK_CLAIM_SUCCESS_STATUSES
    )


def test_lua_task_claim_return_statuses_are_locked_to_python_taxonomy():
    assert (
        _lua_task_claim_return_statuses()
        == dag_lua.TASK_CLAIM_STATUSES
    )


def test_task_claim_result_uses_canonical_success_taxonomy():
    source = inspect.getsource(
        dag_lua.TaskClaimResult.from_lua
    )

    assert "TASK_CLAIM_SUCCESS_STATUSES" in source
    assert 'status == "task_claimed"' not in source

    success = dag_lua.TaskClaimResult.from_lua(
        [b"task_claimed", b"1", b"epoch-1"],
        task_id="task-1",
        worker_id="worker-1",
    )
    failure = dag_lua.TaskClaimResult.from_lua(
        [b"run_truth_missing", b"", b""],
        task_id="task-1",
        worker_id="worker-1",
    )

    assert success.ok is True
    assert failure.ok is False


def test_task_claim_manager_uses_canonical_reservation_status_constants():
    source = Path(
        "hfa-control/src/hfa_control/task_claim.py"
    ).read_text(encoding="utf-8")

    assert "TASK_CLAIM_STATUS_RESERVATION_MISSING" in source
    assert (
        "TASK_CLAIM_STATUS_RESERVATION_WORKER_MISMATCH"
        in source
    )
    assert 'result.status == "reservation_missing"' not in source
    assert 'status="reservation_worker_mismatch"' not in source
