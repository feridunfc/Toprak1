
from hfa.dag.fairness import TenantFairnessSnapshot, fairness_sort_key


def test_normalized_runtime_respects_weight():
    snapshot = TenantFairnessSnapshot(tenant_id="t1", vruntime=0.0, weight=2.0, inflight=0)
    assert snapshot.normalized_runtime(100) == 50.0


def test_normalized_runtime_defaults_when_weight_invalid():
    snapshot = TenantFairnessSnapshot(tenant_id="t1", vruntime=0.0, weight=0.0, inflight=0)
    assert snapshot.normalized_runtime(100) == 100.0


def test_fairness_sort_key_prefers_lower_vruntime_then_load_then_inflight():
    a = TenantFairnessSnapshot(tenant_id="a", vruntime=10.0, weight=1.0, inflight=2)
    b = TenantFairnessSnapshot(tenant_id="b", vruntime=11.0, weight=1.0, inflight=0)

    assert fairness_sort_key(a, worker_load=1, topology_rank=1) < fairness_sort_key(b, worker_load=0, topology_rank=0)
