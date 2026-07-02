
from __future__ import annotations

from pathlib import Path


def test_staging_runtime_scenario_has_one_command_cli_and_artifact_contract() -> None:
    source = Path("scripts/staging_runtime_scenario.py").read_text(encoding="utf-8")

    assert "argparse.ArgumentParser" in source
    assert "DEFAULT_ARTIFACT" in source
    assert "run_staging_runtime_scenario" in source
    assert "json.dumps(artifact, indent=2, sort_keys=True)" in source
    assert "status" in source
    assert "PASS" in source
    assert "BLOCKED" in source


def test_staging_runtime_scenario_reuses_real_runtime_and_evidence_surfaces() -> None:
    source = Path("scripts/staging_runtime_scenario.py").read_text(encoding="utf-8")

    assert "WorkerConsumer" in source
    assert "TaskConsumer" in source
    assert "TaskClaimManager" in source
    assert "WorkerReservationManager" in source
    assert "DagLua" in source
    assert "read_task_evidence" in source
    assert "HFA_WORKER_TASK_CONSUMER_BRIDGE" in source


def test_staging_runtime_scenario_safety_contract() -> None:
    source = Path("scripts/staging_runtime_scenario.py").read_text(encoding="utf-8")

    assert "production_ready_claim" in source
    assert "real_llm_called" in source
    assert "deployment_attempted" in source
    assert "release_tag_created" in source
    assert "retry_or_reclaim_attempted" in source
    assert "runtime_repair_attempted" in source

    forbidden_fragments = [
        "subprocess.run",
        "git tag",
        "git push",
        "docker push",
        "kubectl",
        "helm upgrade",
        "openai",
        "anthropic",
    ]

    for fragment in forbidden_fragments:
        assert fragment not in source
