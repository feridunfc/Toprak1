from __future__ import annotations

import json
from pathlib import Path

import pytest

import scripts.task_execution_product_path as product_path


def _patch_paths(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    artifact_dir = tmp_path / "docs" / "dashboard" / "artifacts"

    monkeypatch.setattr(product_path, "ROOT", tmp_path)
    monkeypatch.setattr(product_path, "ARTIFACT_DIR", artifact_dir)
    monkeypatch.setattr(
        product_path,
        "STORE_PATH",
        artifact_dir / "task_execution_product_store.json",
    )
    monkeypatch.setattr(
        product_path,
        "DEMO_OUTPUT_PATH",
        artifact_dir / "latest_task_execution_demo.json",
    )

    return artifact_dir


def test_tenant_scoped_task_executes_end_to_end(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    artifact_dir = _patch_paths(monkeypatch, tmp_path)

    submitted = product_path.submit_task(
        tenant_id="demo",
        task_type="echo",
        payload={"message": "hello"},
    )

    assert submitted["status"] == "SUBMITTED"
    assert submitted["tenant_id"] == "demo"
    assert submitted["task_type"] == "echo"
    assert submitted["production_llm_call_attempted"] is False
    assert submitted["deployment_attempted"] is False
    assert submitted["release_tag_created"] is False
    assert submitted["noncanonical_redis_mutation_attempted"] is False

    completed = product_path.run_worker_once(tenant_id="demo")

    assert completed["status"] == "COMPLETED"
    assert completed["tenant_id"] == "demo"
    assert completed["task_id"] == submitted["task_id"]
    assert completed["run_id"] == submitted["run_id"]
    assert completed["result"] == {"echo": "hello"}
    assert completed["lifecycle"] == [
        "SUBMITTED",
        "QUEUED",
        "CLAIMED",
        "EXECUTED",
        "COMPLETED",
    ]

    result = product_path.get_task_result(run_id=submitted["run_id"])

    assert result["status"] == "COMPLETED"
    assert result["tenant_id"] == "demo"
    assert result["task_id"] == submitted["task_id"]
    assert result["run_id"] == submitted["run_id"]
    assert result["result"] == {"echo": "hello"}

    artifact = product_path.write_demo_artifact(result)

    assert artifact["source"] == "task_execution_demo"
    assert artifact["status"] == "PASS"
    assert artifact["tenant_id"] == "demo"
    assert artifact["task_id"] == submitted["task_id"]
    assert artifact["run_id"] == submitted["run_id"]
    assert artifact["task_type"] == "echo"
    assert artifact["result"] == {"echo": "hello"}
    assert artifact["fallback_mode"] == product_path.FALLBACK_MODE
    assert "No stable repository task submit/worker entrypoint" in artifact["fallback_reason"]
    assert artifact["canonical_task_lifecycle_mutation"] is True
    assert artifact["production_llm_call_attempted"] is False
    assert artifact["deployment_attempted"] is False
    assert artifact["release_tag_created"] is False
    assert artifact["noncanonical_redis_mutation_attempted"] is False
    assert artifact["operator_action_buttons"] is False
    assert artifact["actionable"] is False
    assert artifact["actions"] == []

    written = json.loads(
        (artifact_dir / "latest_task_execution_demo.json").read_text(encoding="utf-8")
    )
    assert written == artifact


def test_worker_claims_exactly_one_task_for_tenant(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _patch_paths(monkeypatch, tmp_path)

    first = product_path.submit_task(
        tenant_id="demo",
        task_type="echo",
        payload={"message": "first"},
    )
    second = product_path.submit_task(
        tenant_id="demo",
        task_type="echo",
        payload={"message": "second"},
    )

    completed = product_path.run_worker_once(tenant_id="demo")

    assert completed["status"] == "COMPLETED"
    assert completed["run_id"] == first["run_id"]
    assert completed["result"] == {"echo": "first"}

    first_result = product_path.get_task_result(first["run_id"])
    second_result = product_path.get_task_result(second["run_id"])

    assert first_result["status"] == "COMPLETED"
    assert second_result["status"] == "QUEUED"


def test_worker_is_tenant_scoped(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _patch_paths(monkeypatch, tmp_path)

    alpha = product_path.submit_task(
        tenant_id="alpha",
        task_type="echo",
        payload={"message": "alpha-msg"},
    )
    beta = product_path.submit_task(
        tenant_id="beta",
        task_type="echo",
        payload={"message": "beta-msg"},
    )

    completed_beta = product_path.run_worker_once(tenant_id="beta")

    assert completed_beta["status"] == "COMPLETED"
    assert completed_beta["tenant_id"] == "beta"
    assert completed_beta["run_id"] == beta["run_id"]
    assert completed_beta["result"] == {"echo": "beta-msg"}

    alpha_result = product_path.get_task_result(alpha["run_id"])
    assert alpha_result["status"] == "QUEUED"


def test_run_demo_writes_dashboard_artifact(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    artifact_dir = _patch_paths(monkeypatch, tmp_path)

    output = product_path.run_demo(
        tenant_id="demo",
        payload={"message": "hello"},
    )

    assert output["status"] == "PASS"
    assert output["submitted"]["status"] == "SUBMITTED"
    assert output["completed"]["status"] == "COMPLETED"
    assert output["result"]["status"] == "COMPLETED"
    assert output["artifact"]["status"] == "PASS"
    assert output["artifact"]["result"] == {"echo": "hello"}

    artifact_path = artifact_dir / "latest_task_execution_demo.json"
    assert artifact_path.exists()

    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
    assert artifact["status"] == "PASS"
    assert artifact["result"] == {"echo": "hello"}
    assert artifact["fallback_mode"] == product_path.FALLBACK_MODE


def test_submit_rejects_unsupported_task_type(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _patch_paths(monkeypatch, tmp_path)

    with pytest.raises(ValueError, match="unsupported task type"):
        product_path.submit_task(
            tenant_id="demo",
            task_type="real_llm",
            payload={"message": "nope"},
        )


def test_get_result_not_found_is_safe(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _patch_paths(monkeypatch, tmp_path)

    result = product_path.get_task_result("missing-run")

    assert result["status"] == "NOT_FOUND"
    assert result["run_id"] == "missing-run"
    assert result["production_llm_call_attempted"] is False
    assert result["deployment_attempted"] is False
    assert result["release_tag_created"] is False
    assert result["noncanonical_redis_mutation_attempted"] is False
    assert result["actionable"] is False
    assert result["actions"] == []


def test_cli_demo_supports_message_without_json_quoting(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    artifact_dir = _patch_paths(monkeypatch, tmp_path)

    assert product_path.main_args(["demo", "--tenant", "demo", "--message", "hello"]) == 0

    captured = capsys.readouterr()
    output = json.loads(captured.out)

    assert output["status"] == "PASS"
    assert output["artifact"]["result"] == {"echo": "hello"}
    assert (artifact_dir / "latest_task_execution_demo.json").exists()
