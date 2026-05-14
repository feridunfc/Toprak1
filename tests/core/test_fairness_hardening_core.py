
from hfa.dag.fairness_hardening import FairnessPolicy


def test_max_inflight_policy_blocks_when_limit_reached():
    policy = FairnessPolicy(max_inflight_per_tenant=2)
    assert policy.inflight_allowed(0) is True
    assert policy.inflight_allowed(1) is True
    assert policy.inflight_allowed(2) is False


def test_starvation_recovery_never_goes_below_floor():
    policy = FairnessPolicy(starvation_recovery_delta=50.0, vruntime_floor=10.0)
    assert policy.apply_starvation_recovery(100.0) == 50.0
    assert policy.apply_starvation_recovery(40.0) == 10.0


def test_effective_vruntime_respects_floor():
    policy = FairnessPolicy(vruntime_floor=5.0)
    assert policy.effective_vruntime(2.0) == 5.0
    assert policy.effective_vruntime(7.0) == 7.0
