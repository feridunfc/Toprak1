
from hfa.dag.schema import DagRedisKey

def test_worker_reservation_key_shape():
    assert DagRedisKey.worker_reservation("worker-1") == "hfa:dag:worker:worker-1:reservation"

def test_worker_reservation_pattern_shape():
    assert DagRedisKey.worker_reservation_pattern() == "hfa:dag:worker:*:reservation"


def test_task_reservation_owner_key_shape():
    assert DagRedisKey.task_reservation_owner("task-1") == "hfa:dag:task:task-1:reservation_owner"

def test_task_reservation_owner_pattern_shape():
    assert DagRedisKey.task_reservation_owner_pattern() == "hfa:dag:task:*:reservation_owner"
