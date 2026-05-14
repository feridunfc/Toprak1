
from hfa.dag.schema import DagRedisKey

def test_worker_reservation_key_shape():
    assert DagRedisKey.worker_reservation("worker-1") == "hfa:dag:worker:worker-1:reservation"

def test_worker_reservation_pattern_shape():
    assert DagRedisKey.worker_reservation_pattern() == "hfa:dag:worker:*:reservation"
