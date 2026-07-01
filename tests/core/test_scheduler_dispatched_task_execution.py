from __future__ import annotations

import json

import pytest

import scripts.scheduler_dispatched_task_execution as scheduler_proof


def _safe_base_artifact(**overrides):
    artifact = {
        "manual_worker_message_injection_used": False,
        "production_llm_call_attempted": False,
        "noncanonical_redis_mutation_attempted": False,
        "deployment_attempted": False,
        "release_tag_created": False,
        "operator_action_buttons": False,
        "fallback_used": False,
        "product_fallback_used": False,
    }
    artifact.update(overrides)
    return artifact


@pytest.mark.asyncio
async def test_scheduler_dispatched_task_execution_passes_through_dispatch_and_worker() -> None:
    artifact = await scheduler_proof.build_artifact()

    assert artifact["status"] == "PASS"
    assert artifact["tenant_task_submitted"] is True
    assert artifact["canonical_enqueue_used"] is True
    assert artifact["scheduler_dispatch_used"] is True
    assert artifact["dispatch_output_created"] is True
    assert artifact["dispatch_committed"] is True
    assert artifact["dispatch_status"] == "committed"
    assert artifact["run_requested_event_from_dispatch"] is True
    assert artifact["manual_worker_message_injection_used"] is False
    assert artifact["dispatch_scheduler_epoch"]
    assert artifact["dispatch_message_scheduler_epoch"] == artifact["dispatch_scheduler_epoch"]
    assert artifact["worker_event_scheduler_epoch"] == artifact["dispatch_scheduler_epoch"]

    assert artifact["worker_consumer_process_message_used"] is True
    assert artifact["idempotency_guard_claimed"] is True
    assert artifact["fake_executor_used"] is True
    assert artifact["worker_executor_invoked"] is True
    assert artifact["state_store_result_written"] is True
    assert artifact["state_store_transition_state_called"] is True
    assert artifact["state_store_mark_completed_called"] is True
    assert artifact["message_acknowledged"] is True
    assert artifact["result_readable"] is True

    assert artifact["fallback_used"] is False
    assert artifact["product_fallback_used"] is False
    assert artifact["scheduler_lua_python_fallback_used"] is True

    assert artifact["production_llm_call_attempted"] is False
    assert artifact["deployment_attempted"] is False
    assert artifact["release_tag_created"] is False
    assert artifact["noncanonical_redis_mutation_attempted"] is False
    assert artifact["operator_action_buttons"] is False
    assert artifact["failing_reasons"] == []


@pytest.mark.asyncio
async def test_scheduler_dispatched_task_execution_dispatch_message_is_from_scheduler_helper() -> None:
    artifact = await scheduler_proof.build_artifact()

    message = artifact["dispatch_message"]
    assert artifact["scheduler_helper_used"] == "SchedulerLua.dispatch_commit_detailed"
    assert artifact["shard_stream"] == "hfa:stream:runs:0"
    assert artifact["dispatch_message_id"]

    assert message["event_type"] == "RunRequested"
    assert message["run_id"] == artifact["run_id"]
    assert message["tenant_id"] == artifact["tenant_id"]
    assert message["agent_type"] == "fake"
    assert message["worker_group"] == "default"
    assert message["shard"] == "0"
    assert message["scheduler_epoch"] == artifact["dispatch_scheduler_epoch"]

    payload = json.loads(message["payload_json"])
    assert payload == {"prompt": "hello scheduler dispatch"}


def test_scheduler_dispatched_task_execution_safety_policy_accepts_safe_artifact() -> None:
    failing = scheduler_proof.evaluate_safety(_safe_base_artifact())

    assert failing == []


def test_scheduler_dispatched_task_execution_safety_policy_rejects_manual_injection() -> None:
    failing = scheduler_proof.evaluate_safety(
        _safe_base_artifact(manual_worker_message_injection_used=True)
    )

    assert "manual_worker_message_injection_used is not false" in failing


def test_scheduler_dispatched_task_execution_safety_policy_rejects_production_llm() -> None:
    failing = scheduler_proof.evaluate_safety(
        _safe_base_artifact(production_llm_call_attempted=True)
    )

    assert "production_llm_call_attempted is not false" in failing


def test_scheduler_dispatched_task_execution_safety_policy_rejects_noncanonical_redis_mutation() -> None:
    failing = scheduler_proof.evaluate_safety(
        _safe_base_artifact(noncanonical_redis_mutation_attempted=True)
    )

    assert "noncanonical_redis_mutation_attempted is not false" in failing


def test_scheduler_dispatched_task_execution_safety_policy_rejects_deploy_release_and_actions() -> None:
    failing = scheduler_proof.evaluate_safety(
        _safe_base_artifact(
            deployment_attempted=True,
            release_tag_created=True,
            operator_action_buttons=True,
        )
    )

    assert "deployment_attempted is not false" in failing
    assert "release_tag_created is not false" in failing
    assert "operator_action_buttons is not false" in failing


@pytest.mark.asyncio
async def test_scheduler_dispatched_task_execution_writes_dashboard_artifact(tmp_path, monkeypatch) -> None:
    output = tmp_path / "latest_scheduler_dispatched_task_execution.json"
    monkeypatch.setattr(scheduler_proof, "OUTPUT_PATH", output)

    artifact = await scheduler_proof.build_artifact()
    scheduler_proof.write_artifact(artifact)

    assert output.exists()
    data = json.loads(output.read_text(encoding="utf-8"))
    assert data["source"] == "scheduler_dispatched_task_execution"
    assert data["status"] == "PASS"
    assert data["scheduler_dispatch_used"] is True
    assert data["dispatch_output_created"] is True
    assert data["run_requested_event_from_dispatch"] is True
    assert data["manual_worker_message_injection_used"] is False