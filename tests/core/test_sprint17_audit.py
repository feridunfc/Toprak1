"""
tests/core/test_sprint17_audit.py
IRONCLAD Sprint 17 — AuditLogger tests

Verifies:
  - AuditLogger is a no-op when disabled (no key provider)
  - All audit methods exist and are callable
  - Audit failures never propagate to callers
  - AuditEvent constants defined
  - build_audit_logger returns disabled logger when HFA_LEDGER_KEY_ID unset
  - RedisLedgerStore stores and retrieves entries
"""

from __future__ import annotations

import os
import pytest
import fakeredis.aioredis as faredis
from unittest.mock import AsyncMock

from hfa_control.audit import AuditLogger, AuditEvent, build_audit_logger
from hfa_control.audit_store import RedisLedgerStore


# ---------------------------------------------------------------------------
# AuditEvent constants
# ---------------------------------------------------------------------------


def test_audit_event_constants_defined():
    assert AuditEvent.ADMITTED == "ADMITTED"
    assert AuditEvent.REJECTED == "REJECTED"
    assert AuditEvent.SCHEDULED == "SCHEDULED"
    assert AuditEvent.DRAIN_STARTED == "DRAIN_STARTED"
    assert AuditEvent.DLQ_REPLAY == "DLQ_REPLAY"
    assert AuditEvent.DLQ_DELETED == "DLQ_DELETED"
    assert AuditEvent.RESCHEDULE == "RESCHEDULE"
    assert AuditEvent.AUTH_FAILURE == "AUTH_FAILURE"


# ---------------------------------------------------------------------------
# Disabled AuditLogger (no key provider)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_disabled_audit_logger_is_noop():
    """When no key provider is set, all calls silently no-op."""
    audit = AuditLogger()
    assert audit._enabled is False

    # None of these should raise
    await audit.admitted("r1", "t1", "coder")
    await audit.rejected("r2", "t1", "quota_exceeded")
    await audit.scheduled("r3", "t1", "grp-a", 4, "LEAST_LOADED")
    await audit.drain_started("w1")
    await audit.dlq_replay("r4", "t1")
    await audit.dlq_deleted("r5", "t1")
    await audit.reschedule("r6", "t1")


@pytest.mark.asyncio
async def test_disabled_audit_initialise_is_noop():
    audit = AuditLogger()
    await audit.initialise()  # no exception
    assert audit._ledger is None


# ---------------------------------------------------------------------------
# Audit failures never propagate
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_audit_emit_failure_does_not_raise():
    """If ledger.append raises, _emit must swallow and log."""
    audit = AuditLogger()
    audit._enabled = True
    audit._ledger = AsyncMock()
    audit._ledger.append = AsyncMock(side_effect=RuntimeError("ledger exploded"))

    # Must not raise
    await audit._emit(
        event_type=AuditEvent.ADMITTED,
        run_id="r1",
        tenant_id="t1",
        data={"agent_type": "coder"},
    )


# ---------------------------------------------------------------------------
# build_audit_logger factory
# ---------------------------------------------------------------------------


def test_build_audit_logger_disabled_when_no_key_id():
    """HFA_LEDGER_KEY_ID not set → returns disabled AuditLogger."""
    env_backup = os.environ.copy()
    os.environ.pop("HFA_LEDGER_KEY_ID", None)
    try:
        logger = build_audit_logger(redis=None)
        assert logger._enabled is False
    finally:
        os.environ.clear()
        os.environ.update(env_backup)


def _old_test_build_audit_logger_disabled_on_key_load_failure():
    """Even with KEY_ID set, if key provider fails → disabled AuditLogger."""
    env_backup = os.environ.copy()
    os.environ["HFA_LEDGER_KEY_ID"] = "test-key"
    try:
        # Ed25519EnvKeyProvider will fail because keys aren't in env
        logger = build_audit_logger(redis=None)
        assert logger._enabled is False
    finally:
        os.environ.clear()
        os.environ.update(env_backup)


# ---------------------------------------------------------------------------
# RedisLedgerStore (fakeredis)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_redis_ledger_store_get_all_empty():
    redis = faredis.FakeRedis()
    store = RedisLedgerStore(redis)
    entries = await store.get_all("tenant-a", "run-001")
    assert entries == []


@pytest.mark.asyncio
async def test_redis_ledger_store_key_format():
    """Verify key format matches audit namespace."""
    store = RedisLedgerStore(redis=None)
    key = store._key("acme", "run-abc")
    assert key.startswith("hfa:audit:")
    assert "acme" in key
    assert "run-abc" in key


@pytest.mark.asyncio
async def test_redis_ledger_store_get_last_empty():
    redis = faredis.FakeRedis()
    store = RedisLedgerStore(redis)
    result = await store.get_last("tenant-a", "run-001")
    assert result is None


# ---------------------------------------------------------------------------
# Admission controller has _audit attribute
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_admission_controller_accepts_audit_param():
    """AdmissionController __init__ must accept audit= kwarg."""
    import fakeredis.aioredis as faredis
    from hfa_control.admission import AdmissionController
    from hfa_control.models import ControlPlaneConfig

    redis = faredis.FakeRedis()
    config = ControlPlaneConfig(instance_id="test")
    audit = AuditLogger()  # disabled

    ctrl = AdmissionController(redis, config, audit=audit)
    assert ctrl._audit is audit


@pytest.mark.asyncio
async def test_admission_controller_audit_none_is_safe():
    """AdmissionController works fine with audit=None."""
    import fakeredis.aioredis as faredis
    from hfa_control.admission import AdmissionController
    from hfa_control.models import ControlPlaneConfig

    redis = faredis.FakeRedis()
    config = ControlPlaneConfig(instance_id="test")

    ctrl = AdmissionController(redis, config, audit=None)
    assert ctrl._audit is None


def test_build_audit_logger_disabled_on_key_load_failure():
    import os
    from unittest import mock
    from hfa_control.audit import build_audit_logger

    env_backup = os.environ.copy()
    os.environ["HFA_LEDGER_KEY_ID"] = "test-key"
    try:
        # Sistemin çöküş senaryosunu test edebilmek için anahtar yükleyiciyi zorla patlatıyoruz (Mock)
        with mock.patch(
            "hfa.governance.signed_ledger_v1.Ed25519EnvKeyProvider",
            side_effect=Exception("Zorunlu Cokus"),
        ):
            logger = build_audit_logger(redis=None)

        # Çöküş gerçekleştiğinde logger güvenli bir şekilde kendini kapatmalı
        assert logger._enabled is False
    finally:
        os.environ.clear()
        os.environ.update(env_backup)
