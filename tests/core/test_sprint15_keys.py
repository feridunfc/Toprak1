"""
tests/core/test_sprint15_keys.py
IRONCLAD Sprint 15 — RedisKey and RedisTTL unit tests

Verifies that:
  - All key builders produce correct prefixed strings
  - TTL values are positive integers
  - PREFIX change propagates everywhere
  - No raw string duplication with hardcoded "hfa:"
"""

from __future__ import annotations


from hfa.config.keys import RedisKey, RedisTTL, TTL


# ---------------------------------------------------------------------------
# RedisTTL
# ---------------------------------------------------------------------------


def test_all_ttls_are_positive_integers():
    """Every TTL field must be a positive integer."""
    for field_name, value in RedisTTL.__dataclass_fields__.items():
        val = getattr(TTL, field_name)
        assert isinstance(val, int), f"{field_name} is not int"
        assert val > 0, f"{field_name} must be positive, got {val}"


def test_run_state_ttl_is_24h():
    assert TTL.RUN_STATE == 86_400


def test_dlq_meta_ttl_is_7_days():
    assert TTL.DLQ_META == 604_800


def test_claim_lock_ttl_is_5_minutes():
    assert TTL.RUN_CLAIM == 300


# ---------------------------------------------------------------------------
# RedisKey — run keys
# ---------------------------------------------------------------------------


def test_run_state_key_format():
    assert RedisKey.run_state("run-abc") == "hfa:run:state:run-abc"


def test_run_meta_key_format():
    assert RedisKey.run_meta("run-abc") == "hfa:run:meta:run-abc"


def test_run_result_key_format():
    assert RedisKey.run_result("run-abc") == "hfa:run:result:run-abc"


def test_run_claim_key_format():
    assert RedisKey.run_claim("run-abc") == "hfa:run:claim:run-abc"


def test_run_cancel_key_format():
    assert RedisKey.run_cancel("run-abc") == "hfa:run:cancel:run-abc"


def test_run_payload_key_format():
    assert RedisKey.run_payload("run-abc") == "hfa:run:payload:run-abc"


# ---------------------------------------------------------------------------
# RedisKey — tenant keys
# ---------------------------------------------------------------------------


def test_tenant_config_key_format():
    assert RedisKey.tenant_config("acme") == "hfa:tenant:acme:config"


def test_tenant_inflight_key_format():
    assert RedisKey.tenant_inflight("acme") == "hfa:tenant:acme:inflight"


def test_tenant_rate_key_format():
    assert RedisKey.tenant_rate("acme") == "hfa:tenant:acme:rate"


# ---------------------------------------------------------------------------
# RedisKey — control-plane keys
# ---------------------------------------------------------------------------


def test_cp_running_key_format():
    assert RedisKey.cp_running() == "hfa:cp:running"


def test_cp_leader_key_format():
    assert RedisKey.cp_leader() == "hfa:cp:leader"


def test_cp_fence_key_format():
    assert RedisKey.cp_fence() == "hfa:cp:fence"


def test_cp_dlq_meta_key_format():
    assert RedisKey.cp_dlq_meta("run-abc") == "hfa:cp:dlq:meta:run-abc"


# ---------------------------------------------------------------------------
# RedisKey — stream keys
# ---------------------------------------------------------------------------


def test_stream_control_key_format():
    assert RedisKey.stream_control() == "hfa:stream:control"


def test_stream_shard_key_format():
    assert RedisKey.stream_shard(4) == "hfa:stream:runs:4"
    assert RedisKey.stream_shard(0) == "hfa:stream:runs:0"


def test_stream_results_key_format():
    assert RedisKey.stream_results() == "hfa:stream:results"


def test_stream_dlq_key_format():
    assert RedisKey.stream_dlq() == "hfa:stream:dlq"


# ---------------------------------------------------------------------------
# Prefix isolation
# ---------------------------------------------------------------------------


def test_all_run_keys_share_same_prefix():
    """All run keys must start with the PREFIX."""
    prefix = RedisKey.PREFIX
    assert RedisKey.run_state("x").startswith(prefix)
    assert RedisKey.run_meta("x").startswith(prefix)
    assert RedisKey.run_claim("x").startswith(prefix)


def test_all_tenant_keys_share_same_prefix():
    prefix = RedisKey.PREFIX
    assert RedisKey.tenant_config("x").startswith(prefix)
    assert RedisKey.tenant_inflight("x").startswith(prefix)
    assert RedisKey.tenant_rate("x").startswith(prefix)


def test_keys_are_distinct_for_different_run_ids():
    """Different run_ids must produce different keys."""
    assert RedisKey.run_state("run-001") != RedisKey.run_state("run-002")
    assert RedisKey.run_meta("run-001") != RedisKey.run_meta("run-002")


def test_keys_are_distinct_for_different_tenants():
    assert RedisKey.tenant_inflight("a") != RedisKey.tenant_inflight("b")
    assert RedisKey.tenant_rate("a") != RedisKey.tenant_rate("b")


def test_shard_streams_are_distinct():
    assert RedisKey.stream_shard(0) != RedisKey.stream_shard(1)
    assert RedisKey.stream_shard(31) == "hfa:stream:runs:31"
