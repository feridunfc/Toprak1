import json

import pytest

from scripts.canonical_worker_task_execution import (
    OUTPUT_PATH,
    build_artifact,
    write_artifact,
)


@pytest.mark.asyncio
async def test_canonical_worker_task_execution_uses_worker_consumer_path():
    artifact = await build_artifact()

    assert artifact["status"] == "PASS"
    assert artifact["source"] == "canonical_worker_task_execution"

    assert artifact["canonical_worker_consumer_used"] is True
    assert artifact["worker_consumer_process_message_used"] is True
    assert artifact["artifact_backed_safe_local_adapter_used"] is False
    assert artifact["worker_claim_execute_complete_fallback_used"] is False

    assert artifact["run_requested_event_serialized"] is True
    assert artifact["run_requested_event_deserialized"] is True

    assert artifact["idempotency_guard_claimed"] is True
    assert artifact["idempotency_guard_should_execute_checked"] is True

    assert artifact["worker_executor_invoked"] is True
    assert artifact["fake_executor_used"] is True
    assert artifact["executor"] == "FakeExecutor"

    assert artifact["state_store_result_written"] is True
    assert artifact["state_store_transition_state_called"] is True
    assert artifact["state_store_mark_completed_called"] is True

    assert artifact["message_acknowledged"] is True
    assert artifact["result_event_written"] is True
    assert artifact["result_readable"] is True

    assert artifact["tenant_id"] == "tenant-demo"
    assert artifact["run_id"] == "run-canonical-worker-demo"
    assert artifact["agent_type"] == "fake"

    assert artifact["result"]["result"] == "success"
    assert artifact["result"]["input"]["prompt"] == "hello canonical worker"

    assert len(artifact["executor_invocations"]) == 1
    assert artifact["executor_invocations"][0]["tenant_id"] == "tenant-demo"
    assert artifact["executor_invocations"][0]["run_id"] == "run-canonical-worker-demo"

    assert len(artifact["calls"]["try_claim"]) == 1
    assert artifact["calls"]["try_claim"][0]["run_id"] == "run-canonical-worker-demo"
    assert artifact["calls"]["try_claim"][0]["worker_id"] == "worker-canonical-demo"

    assert len(artifact["calls"]["store_result"]) == 1
    assert artifact["calls"]["store_result"][0]["run_id"] == "run-canonical-worker-demo"
    assert artifact["calls"]["store_result"][0]["tenant_id"] == "tenant-demo"
    assert artifact["calls"]["store_result"][0]["status"] == "done"

    assert artifact["calls"]["mark_completed"] == ["run-canonical-worker-demo"]
    assert artifact["calls"]["transition_state"] == [
        {"run_id": "run-canonical-worker-demo", "state": "done"}
    ]

    assert [list(call) for call in artifact["redis_xack_calls"]] == [
        ["hfa:stream:runs:0", "worker_consumers", "1-0"]
    ]
    assert artifact["redis_xadd_calls_count"] == 1


@pytest.mark.asyncio
async def test_canonical_worker_task_execution_preserves_safety_boundaries():
    artifact = await build_artifact()

    assert artifact["production_llm_call_attempted"] is False
    assert artifact["deployment_attempted"] is False
    assert artifact["release_tag_created"] is False
    assert artifact["noncanonical_redis_mutation_attempted"] is False
    assert artifact["operator_action_buttons"] is False

    assert artifact["failing_reasons"] == []


@pytest.mark.asyncio
async def test_canonical_worker_task_execution_writes_dashboard_artifact():
    if OUTPUT_PATH.exists():
        OUTPUT_PATH.unlink()

    artifact = await build_artifact()
    write_artifact(artifact)

    assert OUTPUT_PATH.exists()

    written = json.loads(OUTPUT_PATH.read_text(encoding="utf-8"))

    assert written["status"] == "PASS"
    assert written["source"] == "canonical_worker_task_execution"
    assert written["canonical_worker_consumer_used"] is True
    assert written["worker_consumer_process_message_used"] is True
    assert written["artifact_backed_safe_local_adapter_used"] is False
    assert written["worker_claim_execute_complete_fallback_used"] is False
    assert written["state_store_result_written"] is True
    assert written["state_store_mark_completed_called"] is True
    assert written["message_acknowledged"] is True