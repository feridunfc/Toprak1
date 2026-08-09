from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
MAIN = REPO / "hfa-worker/src/hfa_worker/main.py"
RUNTIME = REPO / "hfa-worker/src/hfa_worker/run_finalizing_runtime.py"


def _main() -> str:
    return MAIN.read_text(encoding="utf-8")


def _runtime() -> str:
    return RUNTIME.read_text(encoding="utf-8")


def test_profile_d_injects_existing_terminal_authority_into_redelivery_adapter() -> None:
    source = _main()
    assert '"task_terminal_authority_binding": (' in source
    assert "self._task_terminal_authority_binding" in source
    assert "if worker_consumer_type is RunFinalizingWorkerConsumer" in source
    assert "RunFinalizingWorkerConsumer" in source


def test_reclaim_idle_ms_is_configurable_with_historical_default() -> None:
    source = _main()
    assert 'config.get("reclaim_idle_ms", 60_000)' in source
    assert "reclaim_idle_ms=self._reclaim_idle_ms" in source
    assert "reclaim_idle_ms must be a non-negative integer" in source


def test_runtime_running_recovery_uses_durable_terminal_replay_surface() -> None:
    source = _runtime()
    assert 'state != "running"' in source
    assert "binding.replay_terminal_projection(" in source
    assert 'lookup["worker_instance_id"]' in source
    assert 'lookup["scheduler_epoch"]' in source
    assert 'lookup["claim_epoch"]' in source


def test_claim_generation_lookup_comes_from_runtime_metadata_not_message() -> None:
    source = _runtime()
    assert "DagRedisKey.task_meta(ctx.task_id)" in source
    assert 'meta.get("worker_instance_id"' in source
    assert 'meta.get("scheduler_epoch"' in source
    assert 'meta.get("claim_epoch"' in source
    assert "terminal recovery claim-generation metadata is incomplete" in source


def test_no_durable_terminal_falls_back_to_existing_claim_path() -> None:
    source = _runtime()
    marker = "if exc.status == TASK_TERMINAL_NOT_COMMITTED_STATUS:"
    assert marker in source
    tail = source[source.index(marker):]
    assert "super()._process_message_via_task_consumer(" in tail


def test_durable_terminal_replay_continues_to_run_finalization_before_ack() -> None:
    source = _runtime()
    replay = source.index("binding.replay_terminal_projection(")
    finalize = source.index("await self._finalize_run_and_ack(", replay)
    assert replay < finalize
    assert "authority_worker_instance_id=lookup[\"worker_instance_id\"]" in source
    helper = source[source.index("async def _finalize_run_and_ack"):]
    assert helper.index("await finalize(") < helper.index("await ack_message(")


def test_non_not_committed_recovery_errors_fail_closed_without_legacy_fallback() -> None:
    source = _runtime()
    error_block = source[source.index("except TaskTerminalAuthorityError as exc:"):]
    assert "Canonical terminal projection recovery failed closed" in error_block
    assert "return" in error_block
    assert "DagLua.task_complete" not in source
    assert "run_terminate_from_tasks.lua" not in source


def test_e_recovery_path_does_not_bind_requeue_or_new_lifecycle_authority() -> None:
    source = _runtime() + "\n" + _main()
    for forbidden in (
        "requeue_stale_task",
        "task_requeue.lua",
        "TASK_EXECUTION",
        "TASK_RETRY",
        "TASK_RECOVER",
        "TASK_REDELIVER",
        "WORKER_RETRY",
    ):
        assert forbidden not in source
