from __future__ import annotations

from pathlib import Path


def test_worker_consumer_declares_legacy_stream_claim_boundary() -> None:
    source = Path("hfa-worker/src/hfa_worker/consumer.py").read_text(encoding="utf-8")

    assert "LEGACY_STREAM_CLAIM_COMPATIBILITY_BOUNDARY" in source
    assert "not the canonical TaskConsumer.claim_start path" in source
    assert "try_claim_and_mark_running(" in source
    assert "StateStore.mark_running" in source
    assert "TaskConsumer.consume_once()" in source
    assert "TaskClaimManager.claim_start" in source


def test_worker_consumer_runtime_path_has_gated_canonical_bridge_and_legacy_default() -> None:
    source = Path("hfa-worker/src/hfa_worker/consumer.py").read_text(encoding="utf-8")
    marker = "    async def _process_message("
    if marker not in source:
        raise AssertionError("WorkerConsumer._process_message not found")

    runtime_body = source[source.index(marker):]

    assert "event = deserialize_run_requested(data)" in runtime_body
    assert "if is_worker_task_consumer_bridge_enabled():" in runtime_body
    assert "await self._process_message_via_task_consumer(event, msg_id, stream, shard)" in runtime_body
    assert "started = await self._guard.try_claim_and_mark_running(" in runtime_body
    assert "result = await self._executor.execute(event)" in runtime_body
    assert ".claim_start(" not in runtime_body


def test_idempotency_guard_declares_state_store_compatibility_claim_boundary() -> None:
    source = Path("hfa-worker/src/hfa_worker/idempotency.py").read_text(encoding="utf-8")

    assert "LEGACY_IDEMPOTENCY_GUARD_CLAIM_BOUNDARY" in source
    assert "StateStore.mark_running" in source
    assert "TaskClaimManager.claim_start" in source
    assert "return await self._state.mark_running(" in source


def test_task_consumer_remains_canonical_claim_start_path() -> None:
    source = Path("hfa-worker/src/hfa_worker/task_consumer.py").read_text(encoding="utf-8")

    assert "class TaskConsumer" in source
    assert "async def consume_once(" in source
    assert "claim_start(" in source
    assert "scheduler_epoch=ctx.scheduler_epoch" in source
    assert "executed = await self._executor.execute(ctx)" in source


def test_worker_main_patch_is_placeholder_not_runtime_stream_bridge() -> None:
    source = Path("hfa-worker/src/hfa_worker/worker_main_patch.py").read_text(encoding="utf-8")

    assert "TaskContext is obtained from your existing task poll mechanism" in source
    assert "consumer.consume_once(ctx, claimed_at_ms=...)" in source
    assert "while True:" in source

def test_worker_consumer_bridge_acks_only_after_fenced_completion_result() -> None:
    source = Path("hfa-worker/src/hfa_worker/consumer.py").read_text(encoding="utf-8")
    marker = "    async def _process_message_via_task_consumer("
    if marker not in source:
        raise AssertionError("WorkerConsumer._process_message_via_task_consumer not found")

    bridge_body = source[source.index(marker): source.index("    async def _process_message(", source.index(marker))]

    assert 'completed = getattr(consumed, "completed", None)' in bridge_body
    assert 'if completed is None:' in bridge_body
    assert 'getattr(completed, "completed", False)' in bridge_body
    assert 'await ack_message(self._redis, stream, CONSUMER_GROUP, msg_id)' in bridge_body
    assert bridge_body.index('completed = getattr(consumed, "completed", None)') < bridge_body.index(
        'await ack_message(self._redis, stream, CONSUMER_GROUP, msg_id)'
    )
