from __future__ import annotations

import scripts.production_lua_dispatch_path as lua_proof


def _passing_artifact(**overrides):
    artifact = {
        "redis_backend": "real_redis",
        "target_claim_supported": True,
        "scheduler_lua_initialised": True,
        "dispatch_commit_loader_used": True,
        "dispatch_commit_sha_loaded": True,
        "production_lua_evalsha_path_used": True,
        "scheduler_lua_python_fallback_used": False,
        "dispatch_output_created": True,
        "run_requested_event_from_lua_dispatch": True,
        "worker_consumer_process_message_used": True,
        "state_store_result_written": True,
        "state_store_mark_completed_called": True,
        "message_acknowledged": True,
        "fake_executor_used": True,
        "manual_worker_message_injection_used": False,
        "production_llm_call_attempted": False,
        "deployment_attempted": False,
        "release_tag_created": False,
        "operator_action_buttons": False,
        "noncanonical_redis_mutation_attempted": False,
    }
    artifact.update(overrides)
    return artifact


def test_production_lua_dispatch_path_contract_accepts_valid_pass_artifact() -> None:
    assert lua_proof.evaluate_artifact(_passing_artifact()) == []


def test_production_lua_dispatch_path_contract_rejects_python_fallback_pass() -> None:
    failing = lua_proof.evaluate_artifact(
        _passing_artifact(
            production_lua_evalsha_path_used=False,
            scheduler_lua_python_fallback_used=True,
        )
    )

    assert "production_lua_evalsha_path_used is not true" in failing
    assert "scheduler_lua_python_fallback_used is not false" in failing


def test_production_lua_dispatch_path_contract_rejects_missing_real_redis() -> None:
    failing = lua_proof.evaluate_artifact(
        _passing_artifact(redis_backend="unavailable")
    )

    assert "redis_backend is not real_redis" in failing


def test_production_lua_dispatch_path_contract_rejects_missing_sha() -> None:
    failing = lua_proof.evaluate_artifact(
        _passing_artifact(dispatch_commit_sha_loaded=False)
    )

    assert "dispatch_commit_sha_loaded is not true" in failing


def test_production_lua_dispatch_path_contract_rejects_manual_worker_injection() -> None:
    failing = lua_proof.evaluate_artifact(
        _passing_artifact(manual_worker_message_injection_used=True)
    )

    assert "manual_worker_message_injection_used is not false" in failing


def test_production_lua_dispatch_path_degraded_artifact_does_not_support_claim() -> None:
    artifact = lua_proof._degraded_artifact(
        "real Redis unavailable; production Lua/EVALSHA path not proven"
    )

    assert artifact["status"] == "DEGRADED"
    assert artifact["target_claim_supported"] is False
    assert artifact["production_lua_evalsha_path_used"] is False
    assert artifact["scheduler_lua_python_fallback_used"] is False
