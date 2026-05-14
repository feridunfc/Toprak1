
from hfa.dag.reasons import TASK_OWNER_MISMATCH, TaskReason


def test_owner_mismatch_reason_exists():
    assert TASK_OWNER_MISMATCH == "task_owner_mismatch"
    assert TaskReason.OWNER_MISMATCH == "task_owner_mismatch"
