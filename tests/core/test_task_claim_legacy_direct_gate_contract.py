from pathlib import Path

import hfa_control.dag_lua as dag_lua_module
import hfa_control.task_claim as task_claim_module


def test_dag_lua_passes_legacy_direct_claim_flag():
    source = Path("hfa-control/src/hfa_control/dag_lua.py").read_text(encoding="utf-8")

    assert "HFA_ALLOW_LEGACY_DIRECT_TASK_CLAIM" in source
    assert "_legacy_direct_claim_arg(allow_legacy_direct_claim)" in source
    assert "allow_legacy_direct_claim: bool | None = None" in source


def test_lua_requires_explicit_legacy_direct_claim_flag():
    source = Path("hfa-core/src/hfa/lua/task_claim_start.lua").read_text(encoding="utf-8")

    assert "local allow_legacy_direct_claim = ARGV[8] or '0'" in source
    assert "allow_legacy_direct_claim == '1'" in source
    assert "expected_scheduler_epoch == '' and allow_legacy_direct_claim == '1'" in source


def test_task_claim_service_marks_legacy_direct_claim_explicit():
    source = Path("hfa-control/src/hfa_control/task_claim.py").read_text(encoding="utf-8")

    assert "allow_legacy_direct_claim=True" in source
    assert "allow_legacy_direct_claim: bool | None = None" in source
