import inspect

import hfa_control.task_claim as task_claim_module


def test_task_claim_manager_has_no_worker_reservation_scan_bridge():
    source = inspect.getsource(task_claim_module)

    assert "_find_reservation_worker_for_task" not in source
    assert "scan_iter" not in source
    assert "worker_reservation_pattern" not in source


def test_task_claim_manager_uses_task_indexed_owner_lookup():
    assert hasattr(task_claim_module, "_get_task_reservation_owner")

    source = inspect.getsource(task_claim_module._get_task_reservation_owner)

    assert "DagRedisKey.task_reservation_owner(task_id)" in source
    assert "hgetall" in source
