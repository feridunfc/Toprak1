
from hfa_control.task_ownership import TaskOwnershipFence
from hfa.dag.reasons import TASK_OWNERSHIP_FENCED


def test_empty_owner_is_claimable():
    result = TaskOwnershipFence.check("", "worker-a")
    assert result.ok is True
    assert result.status == "OK"


def test_same_owner_is_allowed():
    result = TaskOwnershipFence.check("worker-a", "worker-a")
    assert result.ok is True
    assert result.status == "OK"


def test_different_owner_is_fenced():
    result = TaskOwnershipFence.check("worker-b", "worker-a")
    assert result.ok is False
    assert result.status == TASK_OWNERSHIP_FENCED
