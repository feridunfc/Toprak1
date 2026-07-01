from pathlib import Path


TASK_CLAIM_SOURCE = Path("hfa-control/src/hfa_control/task_claim.py")


def _method_block(source: str, method_name: str) -> str:
    marker = f"    async def {method_name}("
    start = source.index(marker)
    next_method = source.find("\n    async def ", start + len(marker))
    next_class = source.find("\nclass ", start + len(marker))
    candidates = [i for i in [next_method, next_class] if i != -1]
    end = min(candidates) if candidates else len(source)
    return source[start:end]


def test_task_claim_service_claim_is_fail_closed_not_legacy_direct():
    source = TASK_CLAIM_SOURCE.read_text(encoding="utf-8")
    block = _method_block(source, "claim")

    assert "raise RuntimeError" in block
    assert "allow_legacy_direct_claim=True" not in block
    assert "claim_legacy_direct_for_compatibility" in block
    assert "TaskClaimManager.claim_start()" in block


def test_legacy_direct_claim_has_explicit_compatibility_name():
    source = TASK_CLAIM_SOURCE.read_text(encoding="utf-8")
    block = _method_block(source, "claim_legacy_direct_for_compatibility")

    assert "allow_legacy_direct_claim=True" in block
    assert "Canonical runtime code must not use" in source
    assert "This is intentionally not named claim()." in block


def test_worker_runtime_does_not_reference_legacy_direct_claim_surface():
    worker_root = Path("hfa-worker/src/hfa_worker")
    combined = "\n".join(
        path.read_text(encoding="utf-8")
        for path in worker_root.rglob("*.py")
    )

    assert "allow_legacy_direct_claim" not in combined
    assert "claim_legacy_direct_for_compatibility" not in combined
    assert ".claim(" not in Path("hfa-worker/src/hfa_worker/task_consumer.py").read_text(encoding="utf-8")
    assert "claim_start(" in Path("hfa-worker/src/hfa_worker/task_consumer.py").read_text(encoding="utf-8")


def test_generic_claim_integrations_do_not_use_legacy_compatibility_surface():
    generic_integration_sources = [
        Path("tests/integration/test_task_claim_start_integration.py"),
        Path("tests/integration/test_worker_task_consumer_integration.py"),
        Path("tests/integration/test_capability_routing_integration.py"),
        Path("tests/integration/test_worker_heartbeat_lifecycle_integration.py"),
    ]

    for path in generic_integration_sources:
        source = path.read_text(encoding="utf-8")
        assert "allow_legacy_direct_claim" not in source
        assert "claim_legacy_direct_for_compatibility" not in source
