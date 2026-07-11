from __future__ import annotations

from dataclasses import fields
from pathlib import Path

from hfa.dag.schema import DagTaskDispatchInput


def test_dispatch_input_carries_scheduler_epoch() -> None:
    field_names = {
        item.name
        for item in fields(DagTaskDispatchInput)
    }

    assert "scheduler_epoch" in field_names


def test_dag_lua_does_not_infer_run_id_from_task_id() -> None:
    source = Path(
        "hfa-control/src/hfa_control/dag_lua.py"
    ).read_text(encoding="utf-8")

    assert 'getattr(dispatch, "run_id", task_id)' not in source
