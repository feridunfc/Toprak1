
from types import SimpleNamespace

from hfa_control.backpressure import BackpressureGuard


def test_hysteresis_state_persists_across_calls():
    cfg = SimpleNamespace(backpressure_soft_high=0.90, backpressure_hysteresis_ratio=0.80, backpressure_hard=1.0)
    guard = BackpressureGuard(cfg)

    first = guard.evaluate(inflight=91, capacity=100)
    assert first.throttled is True
    assert guard.is_soft_throttled is True

    second = guard.evaluate(inflight=85, capacity=100)
    assert second.throttled is True
    assert second.reason == "soft_hysteresis"

    third = guard.evaluate(inflight=71, capacity=100)
    assert third.throttled is False
    assert guard.is_soft_throttled is False
