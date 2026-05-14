"""
tests/core/test_sprint21_state_machine.py
IRONCLAD Sprint 21 — State machine + CAS transition tests
"""
import pytest
import fakeredis.aioredis as faredis

from hfa_control.state_machine import (
    validate_transition,
    transition_state,
    is_terminal,
    InvalidStateTransition,
    TERMINAL_STATES,
    VALID_TRANSITIONS,
)
from hfa.config.keys import RedisKey, RedisTTL


# ── validate_transition ────────────────────────────────────────────────────

def test_initial_admitted_ok():
    assert validate_transition("r", None, "admitted") is True

def test_initial_queued_ok():
    assert validate_transition("r", None, "queued") is True

def test_initial_running_illegal():
    with pytest.raises(InvalidStateTransition):
        validate_transition("r", None, "running")

def test_admitted_to_queued():
    assert validate_transition("r", "admitted", "queued") is True

def test_queued_to_scheduled():
    assert validate_transition("r", "queued", "scheduled") is True

def test_scheduled_to_running():
    assert validate_transition("r", "scheduled", "running") is True

def test_running_to_done():
    assert validate_transition("r", "running", "done") is True

def test_running_to_rescheduled():
    assert validate_transition("r", "running", "rescheduled") is True

def test_rescheduled_to_admitted():
    assert validate_transition("r", "rescheduled", "admitted") is True

def test_done_is_terminal():
    assert is_terminal("done") is True
    with pytest.raises(InvalidStateTransition):
        validate_transition("r", "done", "running")

def test_failed_is_terminal():
    assert is_terminal("failed") is True

def test_illegal_queued_to_admitted():
    with pytest.raises(InvalidStateTransition):
        validate_transition("r", "queued", "admitted")

def test_illegal_running_to_queued():
    with pytest.raises(InvalidStateTransition):
        validate_transition("r", "running", "queued")

def test_raise_false_returns_false():
    result = validate_transition("r", "done", "running", raise_on_invalid=False)
    assert result is False

def test_all_terminal_states_no_outgoing():
    for state in TERMINAL_STATES:
        assert VALID_TRANSITIONS[state] == frozenset()


# ── transition_state CAS ───────────────────────────────────────────────────

    # ESKİ HALİ:
    # ok = await transition_state(redis, "run-cas-1", "scheduled", state_key=key)
    # assert ok is True

    # YENİ HALİ (Şununla değiştir):
    @pytest.mark.asyncio
    async def test_transition_state_basic():
        redis = faredis.FakeRedis()
        key = RedisKey.run_state("run-cas-1")
        await redis.set(key, "queued", ex=RedisTTL.RUN_STATE)

        # V22 Lockdown: Mevcut durumu vermeden geçiş yapmak reddedilir (Güvenlik)
        failed_result = await transition_state(redis, "run-cas-1", "scheduled", state_key=key)
        assert failed_result.ok is False
        assert failed_result.reason == "initial_write_blocked"

        # V22 Lockdown: Geçerli expected_state verilirse kabul edilir
        ok_result = await transition_state(redis, "run-cas-1", "scheduled", state_key=key, expected_state="queued")
        assert ok_result.ok is True


@pytest.mark.asyncio
async def test_transition_state_terminal_blocked():
    redis = faredis.FakeRedis()
    key = RedisKey.run_state("run-cas-3")
    await redis.set(key, "done", ex=RedisTTL.RUN_STATE)

    # V22 Lockdown: Fonksiyon artık TransitionResult dönüyor ve
    # expected_state verilmediği için terminal state'i ezip geçemiyor.
    result = await transition_state(redis, "run-cas-3", "running", state_key=key)
    assert result.ok is False
    assert result.reason == "initial_write_blocked"


@pytest.mark.asyncio
async def test_transition_state_expected_match_succeeds():
    redis = faredis.FakeRedis()
    key = RedisKey.run_state("run-cas-4")
    await redis.set(key, "scheduled", ex=RedisTTL.RUN_STATE)

    result = await transition_state(
        redis, "run-cas-4", "running",
        state_key=key, expected_state="scheduled",
    )
    # V22 Lockdown: Objenin 'ok' argümanını kontrol ediyoruz
    assert result.ok is True
    assert result.reason == "committed"


@pytest.mark.asyncio
async def test_transition_state_cas_mismatch():
    redis = faredis.FakeRedis()
    key = RedisKey.run_state("run-cas-2")
    await redis.set(key, "running", ex=RedisTTL.RUN_STATE)

    # Expect "queued" but current is "running"
    result = await transition_state(
        redis, "run-cas-2", "scheduled",
        state_key=key, expected_state="queued",
    )
    # V22 Lockdown: Fonksiyon artık TransitionResult dönüyor
    assert result.ok is False
    assert result.reason == "cas_miss"


@pytest.mark.asyncio
async def test_transition_state_missing_key():
    redis = faredis.FakeRedis()
    key = RedisKey.run_state("run-cas-3")

    # Key does not exist. We expect "queued".
    result = await transition_state(
        redis, "run-cas-3", "scheduled",
        state_key=key, expected_state="queued",
    )
    # V22 Lockdown: Fonksiyon artık TransitionResult dönüyor
    assert result.ok is False
    assert result.reason == "cas_miss"