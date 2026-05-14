
from hfa.dag.capabilities import TaskCapabilitySpec, WorkerCapabilitySpec
from hfa_control.capability_router import CapabilityRouter


def test_capability_match_success():
    task = TaskCapabilitySpec(required_capabilities=["python", "sql"])
    worker = WorkerCapabilitySpec(worker_id="w1", capabilities=["sql", "python", "redis"])
    result = CapabilityRouter.matches(task, worker)
    assert result.ok is True
    assert result.missing_capabilities == []


def test_capability_match_failure():
    task = TaskCapabilitySpec(required_capabilities=["python", "sql"])
    worker = WorkerCapabilitySpec(worker_id="w1", capabilities=["python"])
    result = CapabilityRouter.matches(task, worker)
    assert result.ok is False
    assert result.missing_capabilities == ["sql"]


def test_capability_normalization():
    task = TaskCapabilitySpec(required_capabilities=[" Python ", "python", "SQL"])
    assert task.normalized() == ["python", "sql"]
