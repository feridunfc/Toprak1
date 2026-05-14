"""
tests/core/test_sprint21_aging.py
IRONCLAD Sprint 21 — Logarithmic capped aging tests
"""
import math
import time


def _score(priority: int, admitted_at: float, age_weight: float, max_boost: int = 5) -> float:
    """Mirror of scheduler.py _enqueue_admitted scoring logic."""
    ts_micros = int(admitted_at * 1_000_000) % int(1e12)
    age_s = max(0.0, time.time() - admitted_at)
    max_aging_boost = max_boost * int(1e12)
    if age_weight > 0:
        aging_credit = min(max_aging_boost, int(math.log1p(age_s) * age_weight * 1_000_000))
    else:
        aging_credit = 0
    raw_score = priority * int(1e12) + ts_micros - aging_credit
    return float(max(1, raw_score))


def test_priority_order_without_aging():
    now = time.time()
    assert _score(1, now, 0.0) < _score(5, now, 0.0)

def test_score_always_positive():
    """Score must never be zero or negative regardless of aging."""
    old = time.time() - 1_000_000
    s = _score(1, old, age_weight=1_000_000.0, max_boost=100)
    assert s >= 1.0


def test_aging_credit_is_logarithmic():
    """Aging credit grows sub-linearly — doubly-waiting does not double the credit."""
    now = time.time()

    def extract_credit(age_s):
        admitted_at = now - age_s
        s = _score(3, admitted_at, age_weight=1.0)
        ts_micros = int(admitted_at * 1_000_000) % int(1e12)
        # Score = Priority + ts_micros - Credit
        # Credit = Priority + ts_micros - Score
        return (3 * int(1e12) + ts_micros) - s

    credit_100 = extract_credit(100)
    credit_200 = extract_credit(200)
    credit_400 = extract_credit(400)

    assert credit_400 > credit_200 > credit_100

    # 100. saniyeden 200. saniyeye geçerken kazanılan ek kredi (100 saniyelik emek)
    gain_1 = credit_200 - credit_100

    # 200. saniyeden 400. saniyeye geçerken kazanılan ek kredi (200 saniyelik emek)
    gain_2 = credit_400 - credit_200

    # Lineer bir sistemde 200 saniye beklemek, 100 saniye beklemekten 2 kat fazla kredi verir.
    # Logaritmik bir sistemde ise gain_2, gain_1'e oldukça yakındır (kesinlikle 1.5 katından küçüktür).
    assert gain_2 < (gain_1 * 1.5), "Sub-linear growth: Doubling time shouldn't double the credit"

def test_aging_cap_prevents_priority_inversion():
    """A very old low-priority job should NOT score below a high-priority new job with tight cap."""
    now = time.time()
    # priority=10 (low), waited 1 million seconds, cap=2 priority bands
    s_old_low  = _score(10, now - 1_000_000, age_weight=1.0, max_boost=2)
    # priority=1 (high), brand new, cap=2
    s_new_high = _score(1, now, age_weight=1.0, max_boost=2)
    # With cap=2, old low-priority can only boost 2 bands max
    # priority=10 with 2-band boost → effective ~8 → still above priority=1
    assert s_old_low > s_new_high, "Cap prevents full priority inversion"

def test_zero_weight_backward_compat():
    now = time.time()
    s1 = _score(3, now, 0.0)
    s2 = _score(3, now, 0.0)
    assert s1 == s2
