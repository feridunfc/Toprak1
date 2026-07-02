from pathlib import Path


def test_task_consumer_does_not_use_legacy_direct_claim_flag():
    source = Path("hfa-worker/src/hfa_worker/task_consumer.py").read_text(encoding="utf-8")

    assert "allow_legacy_direct_claim=True" not in source
    assert "allow_legacy_direct_claim" not in source


def test_task_consumer_passes_scheduler_epoch_from_context():
    source = Path("hfa-worker/src/hfa_worker/task_consumer.py").read_text(encoding="utf-8")

    assert "scheduler_epoch=ctx.scheduler_epoch" in source
    assert "claim_start(" in source

def test_generic_task_claim_start_integration_is_not_legacy_surface():
    source = Path("tests/integration/test_task_claim_start_integration.py").read_text(encoding="utf-8")

    assert "allow_legacy_direct_claim=True" not in source
    assert "allow_legacy_direct_claim" not in source


def test_task_consumer_wires_fenced_completion_tuple():
    source = Path("hfa-worker/src/hfa_worker/task_consumer.py").read_text(encoding="utf-8")

    assert "completion_manager" in source
    assert "async def _complete_with_fence(" in source
    assert "expected_scheduler_epoch=scheduler_epoch" in source
    assert "expected_claim_epoch=claim.claim_epoch" in source
    assert "worker_instance_id=ctx.worker_instance_id" in source
    assert "task_id=ctx.task_id" in source
    assert "completed = await self._complete_with_fence(ctx, claim, executed)" in source
