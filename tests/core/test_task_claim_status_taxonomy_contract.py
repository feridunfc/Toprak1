import re
from pathlib import Path

import hfa_control.dag_lua as dag_lua


EXPECTED_TASK_CLAIM_STATUSES = {
    "task_claimed",
    "task_missing",
    "task_already_owned",
    "task_state_conflict",
    "reservation_missing",
    "reservation_worker_mismatch",
    "reservation_task_mismatch",
    "reservation_epoch_mismatch",
}


def _lua_task_claim_return_statuses() -> set[str]:
    source = Path("hfa-core/src/hfa/lua/task_claim_start.lua").read_text(encoding="utf-8")
    return set(re.findall(r"return\s*\{\s*['\"]([^'\"]+)['\"]", source, flags=re.MULTILINE))


def test_python_task_claim_taxonomy_matches_expected_contract():
    assert dag_lua.TASK_CLAIM_STATUSES == EXPECTED_TASK_CLAIM_STATUSES
    assert dag_lua.TASK_CLAIM_SUCCESS_STATUSES == {"task_claimed"}
    assert dag_lua.TASK_CLAIM_FAILURE_STATUSES == EXPECTED_TASK_CLAIM_STATUSES - {"task_claimed"}


def test_lua_task_claim_return_statuses_are_locked_to_python_taxonomy():
    lua_statuses = _lua_task_claim_return_statuses()

    assert lua_statuses == dag_lua.TASK_CLAIM_STATUSES


def test_task_claim_result_uses_canonical_success_taxonomy():
    source = Path("hfa-control/src/hfa_control/dag_lua.py").read_text(encoding="utf-8")

    assert "ok           = status in TASK_CLAIM_SUCCESS_STATUSES" in source
    assert 'status == "task_claimed"' not in source


def test_task_claim_manager_uses_canonical_reservation_status_constants():
    source = Path("hfa-control/src/hfa_control/task_claim.py").read_text(encoding="utf-8")

    assert "TASK_CLAIM_STATUS_RESERVATION_MISSING" in source
    assert "TASK_CLAIM_STATUS_RESERVATION_WORKER_MISMATCH" in source
    assert 'result.status == "reservation_missing"' not in source
    assert 'status="reservation_worker_mismatch"' not in source
