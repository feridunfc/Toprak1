
from hfa.runtime.backpressure import BackpressurePolicy


def test_tenant_pending_limit_blocks_at_threshold():
    policy = BackpressurePolicy(max_pending_per_tenant=3)
    assert policy.tenant_pending_allowed(0) is True
    assert policy.tenant_pending_allowed(2) is True
    assert policy.tenant_pending_allowed(3) is False


def test_global_pending_limit_blocks_at_threshold():
    policy = BackpressurePolicy(global_pending_limit=5)
    assert policy.global_pending_allowed(4) is True
    assert policy.global_pending_allowed(5) is False


def test_running_limit_blocks_at_threshold():
    policy = BackpressurePolicy(max_running_per_tenant=2)
    assert policy.tenant_running_allowed(1) is True
    assert policy.tenant_running_allowed(2) is False
